package saldo_test

import (
	"context"
	"encoding/json"
	"go.mongodb.org/mongo-driver/bson"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	saldo_handler "github.com/MamangRust/monolith-payment-gateway-apigateway/handler/saldo"

	pb "github.com/MamangRust/monolith-payment-gateway-pb/saldo/stats"
	"github.com/MamangRust/monolith-payment-gateway-pkg/logger"
	"github.com/MamangRust/monolith-payment-gateway-saldo/handler"
	"github.com/MamangRust/monolith-payment-gateway-saldo/repository"
	"github.com/MamangRust/monolith-payment-gateway-saldo/service"
	"github.com/MamangRust/monolith-payment-gateway-shared/cache"
	apierror "github.com/MamangRust/monolith-payment-gateway-shared/errors"
	"github.com/MamangRust/monolith-payment-gateway-shared/observability"
	tests "github.com/MamangRust/monolith-payment-gateway-test"
	"github.com/labstack/echo/v4"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

type SaldoStatsHandlerApiTestSuite struct {
	suite.Suite
	ts         *tests.TestSuite
	echo       *echo.Echo
	lis        *bufconn.Listener
	conn       *grpc.ClientConn
	userID     int32
	cardNumber string
	testYear   int
}

func (s *SaldoStatsHandlerApiTestSuite) SetupSuite() {
	ts, err := tests.SetupTestSuite()
	s.Require().NoError(err)
	s.ts = ts

	mongoDB, err := s.ts.MongoDB()
	if err != nil {
		s.Require().NoError(err)
	}
	repos := repository.NewRepositories(mongoDB, nil, nil)

	zapLog := zap.NewNop()
	myLogger := &logger.Logger{Log: zapLog}

	redisOption, err := redis.ParseURL(s.ts.RedisURL)
	s.Require().NoError(err)
	redisClient := redis.NewClient(redisOption)
	cacheStore := cache.NewCacheStore(redisClient, myLogger, &dummyCacheMetrics{})

	svc := service.NewService(&service.Deps{
		Repositories: repos,
		Logger:       myLogger,
		Cache:        cacheStore,
		DB:           mongoDB,
	})

	h := handler.NewHandler(svc)

	s.lis = bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	pb.RegisterSaldoStatsBalanceServiceServer(server, h)

	go func() {
		if err := server.Serve(s.lis); err != nil {
		}
	}()

	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return s.lis.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	s.Require().NoError(err)
	s.conn = conn

	s.echo = echo.New()
	obs, _ := observability.NewObservability("test", myLogger)
	apiHandler := apierror.NewApiHandler(obs, myLogger)

	saldo_handler.RegisterSaldoHandler(&saldo_handler.DepsSaldo{
		Client:     s.conn,
		E:          s.echo,
		Logger:     myLogger,
		Cache:      cacheStore,
		ApiHandler: apiHandler,
	})

	s.testYear = time.Now().Year()

	ctx := context.Background()
	userRes, err := mongoDB.Collection("users").InsertOne(ctx, bson.M{
		"firstname":         "SaldoApi",
		"lastname":          "Stats",
		"email":             "saldo_api_stats@example.com",
		"password":          "pass",
		"verification_code": "123",
		"is_verified":       true,
	})
	s.Require().NoError(err)

	var userDoc struct {
		UserID int32 `bson:"user_id"`
	}
	err = mongoDB.Collection("users").FindOne(ctx, bson.M{"_id": userRes.InsertedID}).Decode(&userDoc)
	s.Require().NoError(err)
	s.userID = userDoc.UserID

	s.cardNumber = "0000111122223333"
	_, err = mongoDB.Collection("cards").InsertOne(ctx, bson.M{
		"user_id":       int32(s.userID),
		"card_number":   s.cardNumber,
		"card_type":     "debit",
		"cvv":           "123",
		"card_provider": "visa",
		"expire_date":   time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC),
	})
	s.Require().NoError(err)

	_, err = mongoDB.Collection("saldos").InsertOne(ctx, bson.M{
		"card_number":   s.cardNumber,
		"total_balance": 3000000,
	})
	s.Require().NoError(err)
}

func (s *SaldoStatsHandlerApiTestSuite) TearDownSuite() {
	if s.conn != nil {
		s.conn.Close()
	}
	if s.lis != nil {
		s.lis.Close()
	}
	s.ts.Teardown()
}

func (s *SaldoStatsHandlerApiTestSuite) TestFindMonthlySaldoBalances() {
	year := strconv.Itoa(s.testYear)
	req := httptest.NewRequest(http.MethodGet, "/api/saldo-stats-balance/monthly-balances?year="+year, nil)
	rec := httptest.NewRecorder()
	s.echo.ServeHTTP(rec, req)

	s.Equal(http.StatusOK, rec.Code)
	var resp map[string]interface{}
	json.Unmarshal(rec.Body.Bytes(), &resp)
	s.Equal("success", resp["status"])
}

func TestSaldoStatsHandlerApiSuite(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	suite.Run(t, new(SaldoStatsHandlerApiTestSuite))
}

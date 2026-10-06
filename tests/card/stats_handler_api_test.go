package card_test

import (
	"context"
	"encoding/json"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	apihandler "github.com/MamangRust/monolith-payment-gateway-apigateway/handler/card"

	"github.com/MamangRust/monolith-payment-gateway-card/handler"
	"github.com/MamangRust/monolith-payment-gateway-card/repository"
	"github.com/MamangRust/monolith-payment-gateway-card/service"
	pb "github.com/MamangRust/monolith-payment-gateway-pb/card"
	pbstats "github.com/MamangRust/monolith-payment-gateway-pb/card/stats"
	"github.com/MamangRust/monolith-payment-gateway-pkg/logger"
	"github.com/MamangRust/monolith-payment-gateway-shared/cache"
	apierror "github.com/MamangRust/monolith-payment-gateway-shared/errors"
	"github.com/MamangRust/monolith-payment-gateway-shared/observability"
	tests "github.com/MamangRust/monolith-payment-gateway-test"
	"github.com/labstack/echo/v4"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/suite"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
)

type CardStatsApiTestSuite struct {
	suite.Suite
	ts          *tests.TestSuite
	echo        *echo.Echo
	mongoDB     *mongo.Database
	cardNumber1 string
	testYear    int
	grpcServer  *grpc.Server
	lis         *bufconn.Listener
	conn        *grpc.ClientConn
}

func (s *CardStatsApiTestSuite) SetupSuite() {
	ts, err := tests.SetupTestSuite()
	s.Require().NoError(err)
	s.ts = ts

	mongoDB, err := s.ts.MongoDB()
	if err != nil {
		s.Require().NoError(err)
	}
	s.mongoDB = mongoDB
	opts, err := redis.ParseURL(s.ts.RedisURL)
	s.Require().NoError(err)
	redisClient := redis.NewClient(opts)

	repos := repository.NewRepositories(mongoDB, nil)

	logger.ResetInstance()
	lp := sdklog.NewLoggerProvider()
	log, _ := logger.NewLogger("test", lp)
	cacheMetrics, _ := observability.NewCacheMetrics("test")
	cacheStore := cache.NewCacheStore(redisClient, log, cacheMetrics)

	cardSvc := service.NewService(&service.Deps{
		Repositories: repos,
		Logger:       log,
		Cache:        cacheStore,
		Kafka:        nil,
	})
	cardH := handler.NewHandler(cardSvc)

	// Setup gRPC server
	s.lis = bufconn.Listen(1024 * 1024)
	s.grpcServer = grpc.NewServer()

	pb.RegisterCardQueryServiceServer(s.grpcServer, cardH)
	pbstats.RegisterCardStatsBalanceServiceServer(s.grpcServer, cardH)
	pbstats.RegisterCardStatsTopupServiceServer(s.grpcServer, cardH)
	pbstats.RegisterCardStatsTransferServiceServer(s.grpcServer, cardH)
	pbstats.RegisterCardStatsWithdrawServiceServer(s.grpcServer, cardH)
	pbstats.RegisterCardStatsTransactionServiceServer(s.grpcServer, cardH)

	go func() {
		if err := s.grpcServer.Serve(s.lis); err != nil {
		}
	}()

	s.conn, err = grpc.DialContext(context.Background(), "bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return s.lis.Dial()
		}),
		grpc.WithInsecure())
	s.Require().NoError(err)

	s.echo = echo.New()
	obs, _ := observability.NewObservability("test", log)
	apiHandler := apierror.NewApiHandler(obs, log)

	apihandler.RegisterCardHandler(&apihandler.DepsCard{
		Client:     s.conn,
		E:          s.echo,
		Logger:     log,
		Cache:      cacheStore,
		ApiHandler: apiHandler,
	})

	s.testYear = time.Now().Year()

	// Seed data
	ctx := context.Background()
	s.cardNumber1 = "4444555566667777"

	_, err = mongoDB.Collection("users").InsertOne(ctx, bson.M{"firstname": "Api", "lastname": "Stats", "email": "api_stats_card@example.com", "password": "pass", "verification_code": "123", "is_verified": true})
	s.Require().NoError(err)

	_, err = mongoDB.Collection("cards").InsertOne(ctx, bson.M{"card_number": s.cardNumber1, "card_type": "debit", "cvv": "123", "card_provider": "visa", "expire_date": time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)})

	s.seedHistoricalData()
}

func (s *CardStatsApiTestSuite) seedHistoricalData() {
	_, err := s.mongoDB.Collection("saldos").InsertOne(context.Background(), bson.M{"card_number": s.cardNumber1, "total_balance": 1000, "created_at": time.Date(s.testYear, 1, 15, 10, 0, 0, 0, time.UTC)})
	s.Require().NoError(err)
	_, err = s.mongoDB.Collection("topups").InsertOne(context.Background(), bson.M{"card_number": s.cardNumber1, "topup_amount": 500, "topup_time": time.Date(s.testYear, 1, 10, 10, 0, 0, 0, time.UTC), "topup_method": "api", "status": "success"})
}

func (s *CardStatsApiTestSuite) TearDownSuite() {
	if s.conn != nil {
		s.conn.Close()
	}
	if s.grpcServer != nil {
		s.grpcServer.Stop()
	}
	if s.ts != nil {
		s.ts.Teardown()
	}
}

func (s *CardStatsApiTestSuite) TestBalanceApi() {
	// Global Monthly
	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/api/card-stats-balance/monthly-balance?year=%d", s.testYear), nil)
	rec := httptest.NewRecorder()
	s.echo.ServeHTTP(rec, req)

	s.Equal(http.StatusOK, rec.Code)

	var response map[string]interface{}
	json.Unmarshal(rec.Body.Bytes(), &response)
	data := response["data"].([]interface{})
	firstMonth := data[0].(map[string]interface{})
	s.Equal(float64(1000), firstMonth["total_balance"])

	// By Card Monthly
	reqByCard := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/api/card-stats-balance/monthly-balance-by-card?year=%d&card_number=%s", s.testYear, s.cardNumber1), nil)
	recByCard := httptest.NewRecorder()
	s.echo.ServeHTTP(recByCard, reqByCard)

	s.Equal(http.StatusOK, recByCard.Code)
}

func (s *CardStatsApiTestSuite) TestTopupApi() {
	req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/api/card-stats-topup/monthly-topup-amount?year=%d", s.testYear), nil)
	rec := httptest.NewRecorder()
	s.echo.ServeHTTP(rec, req)

	s.Equal(http.StatusOK, rec.Code)

	var response map[string]interface{}
	json.Unmarshal(rec.Body.Bytes(), &response)
	data := response["data"].([]interface{})
	firstMonth := data[0].(map[string]interface{})
	s.Equal(float64(500), firstMonth["total_amount"])
}

func TestCardStatsApiSuite(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	suite.Run(t, new(CardStatsApiTestSuite))
}

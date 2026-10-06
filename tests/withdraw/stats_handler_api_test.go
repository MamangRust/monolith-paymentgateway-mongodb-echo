package withdraw_test

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

	card_repo "github.com/MamangRust/monolith-payment-gateway-card/repository"
	saldo_repo "github.com/MamangRust/monolith-payment-gateway-saldo/repository"

	withdraw_handler "github.com/MamangRust/monolith-payment-gateway-apigateway/handler/withdraw"
	pb "github.com/MamangRust/monolith-payment-gateway-pb/withdraw/stats"
	"github.com/MamangRust/monolith-payment-gateway-pkg/logger"
	"github.com/MamangRust/monolith-payment-gateway-shared/cache"
	apierror "github.com/MamangRust/monolith-payment-gateway-shared/errors"
	"github.com/MamangRust/monolith-payment-gateway-shared/observability"
	tests "github.com/MamangRust/monolith-payment-gateway-test"
	"github.com/MamangRust/monolith-payment-gateway-withdraw/handler"
	"github.com/MamangRust/monolith-payment-gateway-withdraw/repository"
	"github.com/MamangRust/monolith-payment-gateway-withdraw/service"
	"github.com/labstack/echo/v4"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/suite"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

type WithdrawStatsHandlerApiTestSuite struct {
	suite.Suite
	ts         *tests.TestSuite
	echo       *echo.Echo
	lis        *bufconn.Listener
	conn       *grpc.ClientConn
	userID     int32
	cardNumber string
	testYear   int
	testMonth  int
}

func (s *WithdrawStatsHandlerApiTestSuite) SetupSuite() {
	ts, err := tests.SetupTestSuite()
	s.Require().NoError(err)
	s.ts = ts
	mongoDB, err := s.ts.MongoDB()
	if err != nil {
		s.Require().NoError(err)
	}

	realSaldo := &realSaldoRepo{repo: saldo_repo.NewRepositories(mongoDB, nil, nil)}
	realCard := &realCardRepo{
		query: card_repo.NewCardQueryRepository(mongoDB.Collection("cards")),
	}

	repos := repository.NewRepositoriesWithAdapters(mongoDB, realCard, realSaldo)

	zapLog := zap.NewNop()
	myLogger := &logger.Logger{Log: zapLog}

	redisOption, err := redis.ParseURL(s.ts.RedisURL)
	s.Require().NoError(err)
	redisClient := redis.NewClient(redisOption)
	cacheStore := cache.NewCacheStore(redisClient, myLogger, &dummyCacheMetrics{})

	svc := service.NewService(&service.Deps{
		Kafka:        nil,
		Repositories: repos,
		Logger:       myLogger,
		Cache:        cacheStore,
		DB:           mongoDB,
	})

	h := handler.NewHandler(svc)

	s.lis = bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	pb.RegisterWithdrawStatsStatusServiceServer(server, h)
	pb.RegisterWithdrawStatsAmountServiceServer(server, h)

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

	withdraw_handler.RegisterWithdrawHandler(&withdraw_handler.DepsWithdraw{
		Client:     s.conn,
		E:          s.echo,
		Logger:     myLogger,
		Cache:      cacheStore,
		ApiHandler: apiHandler,
	})

	s.testYear = time.Now().Year()
	s.testMonth = int(time.Now().Month())

	ctx := context.Background()
	_, err = mongoDB.Collection("users").InsertOne(ctx, bson.M{"firstname": "WithdrawApi", "lastname": "Stats", "email": "withdraw_api_stats@example.com", "password": "pass", "verification_code": "123", "is_verified": true})
	s.Require().NoError(err)

	s.cardNumber = "9999000011112222"
	_, err = mongoDB.Collection("cards").InsertOne(ctx, bson.M{"card_number": s.cardNumber, "card_type": "debit", "cvv": "123", "card_provider": "visa", "expire_date": time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)})
	s.Require().NoError(err)

	_, err = mongoDB.Collection("withdraws").InsertOne(ctx, bson.M{"card_number": s.cardNumber, "withdraw_amount": 300000, "withdraw_time": time.Date(s.testYear, time.Month(s.testMonth), 10, 10, 0, 0, 0, time.UTC), "status": "success"})
	s.Require().NoError(err)
}

func (s *WithdrawStatsHandlerApiTestSuite) TearDownSuite() {
	if s.conn != nil {
		s.conn.Close()
	}
	if s.lis != nil {
		s.lis.Close()
	}
	s.ts.Teardown()
}

func (s *WithdrawStatsHandlerApiTestSuite) TestFindMonthlyWithdrawStatusSuccess() {
	year := strconv.Itoa(s.testYear)
	month := strconv.Itoa(s.testMonth)
	req := httptest.NewRequest(http.MethodGet, "/api/withdraw-stats-status/monthly-success?year="+year+"&month="+month, nil)
	rec := httptest.NewRecorder()
	s.echo.ServeHTTP(rec, req)

	s.Equal(http.StatusOK, rec.Code)
	var resp struct {
		Status string        `json:"status"`
		Data   []interface{} `json:"data"`
	}
	json.Unmarshal(rec.Body.Bytes(), &resp)
	s.Equal("success", resp.Status)
	s.NotEmpty(resp.Data)
}

func (s *WithdrawStatsHandlerApiTestSuite) TestFindYearlyWithdrawStatusSuccess() {
	year := strconv.Itoa(s.testYear)
	req := httptest.NewRequest(http.MethodGet, "/api/withdraw-stats-status/yearly-success?year="+year, nil)
	rec := httptest.NewRecorder()
	s.echo.ServeHTTP(rec, req)

	s.Equal(http.StatusOK, rec.Code)
	var resp struct {
		Status string        `json:"status"`
		Data   []interface{} `json:"data"`
	}
	json.Unmarshal(rec.Body.Bytes(), &resp)
	s.Equal("success", resp.Status)
	s.NotEmpty(resp.Data)
}

func (s *WithdrawStatsHandlerApiTestSuite) TestFindMonthlyWithdrawAmount() {
	year := strconv.Itoa(s.testYear)
	req := httptest.NewRequest(http.MethodGet, "/api/withdraw-stats-amount/monthly-amount?year="+year, nil)
	rec := httptest.NewRecorder()
	s.echo.ServeHTTP(rec, req)

	s.Equal(http.StatusOK, rec.Code)
	var resp struct {
		Status string        `json:"status"`
		Data   []interface{} `json:"data"`
	}
	json.Unmarshal(rec.Body.Bytes(), &resp)
	s.Equal("success", resp.Status)
	s.NotEmpty(resp.Data)
}

func (s *WithdrawStatsHandlerApiTestSuite) TestFindMonthlyWithdrawAmountByCardNumber() {
	year := strconv.Itoa(s.testYear)
	req := httptest.NewRequest(http.MethodGet, "/api/withdraw-stats-amount/monthly-amount-card?year="+year+"&card_number="+s.cardNumber, nil)
	rec := httptest.NewRecorder()
	s.echo.ServeHTTP(rec, req)

	s.Equal(http.StatusOK, rec.Code)
	var resp struct {
		Status string        `json:"status"`
		Data   []interface{} `json:"data"`
	}
	json.Unmarshal(rec.Body.Bytes(), &resp)
	s.Equal("success", resp.Status)
	s.NotEmpty(resp.Data)
}

func TestWithdrawStatsHandlerApiSuite(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	suite.Run(t, new(WithdrawStatsHandlerApiTestSuite))
}

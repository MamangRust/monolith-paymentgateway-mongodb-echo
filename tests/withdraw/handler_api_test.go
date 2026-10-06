package withdraw_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	apierror "github.com/MamangRust/monolith-payment-gateway-shared/errors"

	withdrawhandler "github.com/MamangRust/monolith-payment-gateway-apigateway/handler/withdraw"
	card_handler "github.com/MamangRust/monolith-payment-gateway-card/handler"
	card_repo "github.com/MamangRust/monolith-payment-gateway-card/repository"
	card_service "github.com/MamangRust/monolith-payment-gateway-card/service"
	pbcard "github.com/MamangRust/monolith-payment-gateway-pb/card"
	pbsaldo "github.com/MamangRust/monolith-payment-gateway-pb/saldo"
	pb "github.com/MamangRust/monolith-payment-gateway-pb/withdraw"
	adapter "github.com/MamangRust/monolith-payment-gateway-pkg/adapter"
	"github.com/MamangRust/monolith-payment-gateway-pkg/logger"
	saldo_handler "github.com/MamangRust/monolith-payment-gateway-saldo/handler"
	saldo_repo "github.com/MamangRust/monolith-payment-gateway-saldo/repository"
	saldo_service "github.com/MamangRust/monolith-payment-gateway-saldo/service"
	"github.com/MamangRust/monolith-payment-gateway-shared/cache"
	"github.com/MamangRust/monolith-payment-gateway-shared/domain/requests"
	"github.com/MamangRust/monolith-payment-gateway-shared/observability"
	tests "github.com/MamangRust/monolith-payment-gateway-test"
	user_repo "github.com/MamangRust/monolith-payment-gateway-user/repository"
	"github.com/MamangRust/monolith-payment-gateway-withdraw/handler"
	"github.com/MamangRust/monolith-payment-gateway-withdraw/repository"
	"github.com/MamangRust/monolith-payment-gateway-withdraw/service"
	"github.com/labstack/echo/v4"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/suite"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type WithdrawHandlerTestSuite struct {
	suite.Suite
	ts            *tests.TestSuite
	redisClient   *redis.Client
	grpcServer    *grpc.Server
	commandClient pb.WithdrawCommandServiceClient
	queryClient   pb.WithdrawQueryServiceClient
	conn          *grpc.ClientConn
	router        *echo.Echo
	repos         repository.Repositories
	userRepo      user_repo.UserCommandRepository
	cardRepo      card_repo.CardCommandRepository
	saldoRepo     saldo_repo.Repositories

	customerCardNumber string
	withdrawID         int
}

func (s *WithdrawHandlerTestSuite) SetupSuite() {
	ts, err := tests.SetupTestSuite()
	s.Require().NoError(err)
	s.ts = ts

	mongoDB, err := s.ts.MongoDB()
	if err != nil {
		s.Require().NoError(err)
	}

	// Repositories for seeding and service dependencies
	userRepos := user_repo.NewUserCommandRepository(mongoDB.Collection("users"))
	cardRepos := card_repo.NewRepositories(mongoDB, nil)
	saldoRepos := saldo_repo.NewRepositories(mongoDB, nil, nil)

	s.userRepo = userRepos
	s.cardRepo = cardRepos.CardCommand
	s.saldoRepo = saldoRepos

	s.repos = repository.NewRepositoriesWithAdapters(mongoDB, &realCardRepo{query: cardRepos.CardQuery}, &realSaldoRepo{repo: saldoRepos})

	opts, err := redis.ParseURL(s.ts.RedisURL)
	s.Require().NoError(err)
	s.redisClient = redis.NewClient(opts)

	logger.ResetInstance()
	lp := sdklog.NewLoggerProvider()
	log, _ := logger.NewLogger("test", lp)
	obs, _ := observability.NewObservability("test", log)
	cacheMetrics, _ := observability.NewCacheMetrics("test")
	cacheStore := cache.NewCacheStore(s.redisClient, log, cacheMetrics)

	// Register real card/saldo services on the same gRPC server so the
	// withdraw service adapters can dial to themselves.
	cardSvc := card_service.NewService(&card_service.Deps{
		Cache:        cacheStore,
		Repositories: card_repo.NewRepositories(mongoDB, nil),
		Logger:       log,
		Kafka:        nil,
	})
	cardH := card_handler.NewHandler(cardSvc)

	lis, err := net.Listen("tcp", ":0")
	s.Require().NoError(err)

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	s.Require().NoError(err)
	s.conn = conn

	cardAdapter := adapter.NewCardAdapter(pbcard.NewCardQueryServiceClient(conn), pbcard.NewCardCommandServiceClient(conn))
	saldoAdapter := adapter.NewSaldoAdapter(pbsaldo.NewSaldoQueryServiceClient(conn), pbsaldo.NewSaldoCommandServiceClient(conn))

	saldoSvc := saldo_service.NewService(&saldo_service.Deps{
		Cache:        cacheStore,
		Repositories: saldo_repo.NewRepositories(mongoDB, nil, nil),
		CardAdapter:  cardAdapter,
		Logger:       log,
		DB:           mongoDB,
	})
	saldoH := saldo_handler.NewHandler(saldoSvc)

	withdrawService := service.NewService(&service.Deps{
		Kafka:        nil,
		Repositories: s.repos,
		CardAdapter:  cardAdapter,
		SaldoAdapter: saldoAdapter,
		Logger:       log,
		Cache:        cacheStore,
		DB:           mongoDB,
	})

	// Seed Customer
	customer, err := s.userRepo.CreateUser(context.Background(), &requests.CreateUserRequest{
		FirstName: "Withdraw",
		LastName:  "Customer",
		Email:     fmt.Sprintf("withdraw-%d@test.com", time.Now().UnixNano()),
		Password:  "password123",
	})
	s.Require().NoError(err)

	card, err := s.cardRepo.CreateCard(context.Background(), &requests.CreateCardRequest{
		UserID:       int(customer.UserID),
		CardType:     "debit",
		ExpireDate:   time.Now().AddDate(1, 0, 0),
		CVV:          "999",
		CardProvider: "visa",
	})
	s.Require().NoError(err)
	s.customerCardNumber = card.CardNumber

	_, err = s.saldoRepo.CreateSaldo(context.Background(), &requests.CreateSaldoRequest{
		CardNumber:   s.customerCardNumber,
		TotalBalance: 1000000,
	})
	s.Require().NoError(err)

	// Start gRPC Server
	withdrawHandler := handler.NewHandler(withdrawService)

	server := grpc.NewServer()
	pb.RegisterWithdrawCommandServiceServer(server, withdrawHandler)
	pb.RegisterWithdrawQueryServiceServer(server, withdrawHandler)
	pbcard.RegisterCardQueryServiceServer(server, cardH)
	pbcard.RegisterCardCommandServiceServer(server, cardH)
	pbsaldo.RegisterSaldoQueryServiceServer(server, saldoH)
	pbsaldo.RegisterSaldoCommandServiceServer(server, saldoH)
	s.grpcServer = server

	go func() {
		_ = server.Serve(lis)
	}()

	// Create gRPC Clients (reuse the adapter connection)
	s.commandClient = pb.NewWithdrawCommandServiceClient(conn)
	s.queryClient = pb.NewWithdrawQueryServiceClient(conn)

	// Setup Echo
	s.router = echo.New()
	apiErrorHandler := apierror.NewApiHandler(obs, log)

	withdrawhandler.RegisterWithdrawHandler(&withdrawhandler.DepsWithdraw{
		Client:     conn,
		E:          s.router,
		Logger:     log,
		Cache:      cacheStore,
		ApiHandler: apiErrorHandler,
	})
}

func (s *WithdrawHandlerTestSuite) TearDownSuite() {
	if s.conn != nil {
		s.conn.Close()
	}
	if s.grpcServer != nil {
		s.grpcServer.Stop()
	}
	if s.redisClient != nil {
		s.redisClient.Close()
	}
	if s.ts != nil {
		s.ts.Teardown()
	}
}

func (s *WithdrawHandlerTestSuite) Test1_CreateWithdraw() {
	req := &requests.CreateWithdrawRequest{
		CardNumber:     s.customerCardNumber,
		WithdrawAmount: 100000,
		WithdrawTime:   time.Now(),
	}
	body, _ := json.Marshal(req)

	request := httptest.NewRequest(http.MethodPost, "/api/withdraw-command/create", bytes.NewBuffer(body))
	request.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	s.router.ServeHTTP(rec, request)
	s.Require().Equal(http.StatusOK, rec.Code, rec.Body.String())

	var createRes map[string]interface{}
	json.Unmarshal(rec.Body.Bytes(), &createRes)
	withdrawData := createRes["data"].(map[string]interface{})
	s.withdrawID = int(withdrawData["id"].(float64))

	// Verify balance
	customerSaldo, _ := s.saldoRepo.FindByCardNumber(context.Background(), s.customerCardNumber)
	s.Equal(int64(900000), customerSaldo.TotalBalance)
}

func (s *WithdrawHandlerTestSuite) Test2_FindWithdrawById() {
	s.Require().NotZero(s.withdrawID)

	request := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/api/withdraw-query/%d", s.withdrawID), nil)
	rec := httptest.NewRecorder()
	s.router.ServeHTTP(rec, request)
	s.Equal(http.StatusOK, rec.Code)
}

func (s *WithdrawHandlerTestSuite) Test3_FindAllWithdraws() {
	request := httptest.NewRequest(http.MethodGet, "/api/withdraw-query?page=1&page_size=10", nil)
	rec := httptest.NewRecorder()
	s.router.ServeHTTP(rec, request)
	s.Equal(http.StatusOK, rec.Code)
}

func (s *WithdrawHandlerTestSuite) Test4_UpdateWithdraw() {
	s.Require().NotZero(s.withdrawID)

	req := &requests.UpdateWithdrawRequest{
		WithdrawID:     &s.withdrawID,
		CardNumber:     s.customerCardNumber,
		WithdrawAmount: 150000, // Increase by 50000
		WithdrawTime:   time.Now(),
	}
	body, _ := json.Marshal(req)

	request := httptest.NewRequest(http.MethodPost, fmt.Sprintf("/api/withdraw-command/update/%d", s.withdrawID), bytes.NewBuffer(body))
	request.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	s.router.ServeHTTP(rec, request)
	s.Require().Equal(http.StatusOK, rec.Code, rec.Body.String())

	// Verify adjusted balance (900k - 50k = 850k)
	customerSaldo, _ := s.saldoRepo.FindByCardNumber(context.Background(), s.customerCardNumber)
	s.Equal(int64(850000), customerSaldo.TotalBalance)
}

func (s *WithdrawHandlerTestSuite) Test5_TrashedWithdraw() {
	s.Require().NotZero(s.withdrawID)

	request := httptest.NewRequest(http.MethodPost, fmt.Sprintf("/api/withdraw-command/trashed/%d", s.withdrawID), nil)
	rec := httptest.NewRecorder()
	s.router.ServeHTTP(rec, request)
	s.Equal(http.StatusOK, rec.Code)
}

func (s *WithdrawHandlerTestSuite) Test6_RestoreWithdraw() {
	s.Require().NotZero(s.withdrawID)

	request := httptest.NewRequest(http.MethodPost, fmt.Sprintf("/api/withdraw-command/restore/%d", s.withdrawID), nil)
	rec := httptest.NewRecorder()
	s.router.ServeHTTP(rec, request)
	s.Equal(http.StatusOK, rec.Code)
}

func (s *WithdrawHandlerTestSuite) Test7_PermanentDeleteWithdraw() {
	s.Require().NotZero(s.withdrawID)

	request := httptest.NewRequest(http.MethodDelete, fmt.Sprintf("/api/withdraw-command/permanent/%d", s.withdrawID), nil)
	rec := httptest.NewRecorder()
	s.router.ServeHTTP(rec, request)
	s.Equal(http.StatusOK, rec.Code)
}

func TestWithdrawHandlerSuite(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	suite.Run(t, new(WithdrawHandlerTestSuite))
}

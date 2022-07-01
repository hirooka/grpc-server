package grpcserver.domain.service;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.examples.helloworld.GreeterGrpc;
import io.grpc.examples.helloworld.HelloReply;
import io.grpc.examples.helloworld.HelloRequest;
import io.grpc.protobuf.services.ProtoReflectionService;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
public class GrpcService implements IGrpcService {

    static class GreeterImpl extends GreeterGrpc.GreeterImplBase {

        @Override
        public void sayHelloUnary(HelloRequest req, StreamObserver<HelloReply> responseObserver) {
            log.info("gRPC: Unary");
            final HelloReply reply = HelloReply.newBuilder().setMessage("[Unary] Hello " + req.getName()).build();
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        }

        @Override
        public void sayHelloServerStreaming(HelloRequest request, StreamObserver<HelloReply> responseObserver) {
            log.info("gRPC: Server streaming");
            final HelloReply reply = HelloReply.newBuilder().setMessage("[ServerStreaming] Hello " + request.getName()).build();
            responseObserver.onNext(reply);
            responseObserver.onNext(reply);
            responseObserver.onNext(reply);
            responseObserver.onCompleted();
        }

        @Override
        public StreamObserver<HelloRequest> sayHelloClientStreaming(StreamObserver<HelloReply> responseObserver) {
            log.info("gRPC: Client streaming");
            final List<String> requests = new ArrayList<>();
            return new StreamObserver<HelloRequest>() {
                @Override
                public void onNext(HelloRequest request) {
                    requests.add(request.getName());
                }
                @Override
                public void onError(Throwable t) {
                    // ...
                }
                @Override
                public void onCompleted() {
                    final HelloReply reply = HelloReply.newBuilder().setMessage("[ClientStreaming] Hello " + requests.toString()).build();
                    responseObserver.onNext(reply);
                    responseObserver.onCompleted();
                }
            };
        }

        @Override
        public StreamObserver<HelloRequest> sayHelloBidirectionalStreaming(StreamObserver<HelloReply> responseObserver) {
            log.info("gRPC: Bidirectional streaming");
            return new StreamObserver<HelloRequest>() {
                @Override
                public void onNext(HelloRequest request) {
                    final HelloReply reply = HelloReply.newBuilder().setMessage("[BidirectionalStreaming] Hello " + request.getName()).build();
                    responseObserver.onNext(reply);
                    responseObserver.onNext(reply);
                    responseObserver.onNext(reply);
                }
                @Override
                public void onError(Throwable t) {
                    // ...
                }
                @Override
                public void onCompleted() {
                    responseObserver.onCompleted();
                }
            };
        }
    }

    @PostConstruct
    void init() throws Exception {
        final Server server = ServerBuilder.forPort(6565)
                .addService(new GreeterImpl())
                .addService(ProtoReflectionService.newInstance())
                .build();
        server.start();
        //server.awaitTermination();
    }
}

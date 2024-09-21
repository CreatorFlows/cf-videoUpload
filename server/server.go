package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/joho/godotenv"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	awsClient "github.com/creatorflows/cf-videoUpload/aws"
	"github.com/creatorflows/cf-videoUpload/config"
	"github.com/creatorflows/cf-videoUpload/logger"
	proto "github.com/creatorflows/cf-videoUpload/proto"
)

func init() {
	if err := godotenv.Load(); err != nil {
		log.Fatalf("error loading env: %v", err)
		os.Exit(1)
	}
}

type server struct {
	proto.UnimplementedVideoUploadServiceServer
}

func (s *server) Upload(stream proto.VideoUploadService_UploadServer) error {
	svc := awsClient.S3_CLIENT
	bucketName := os.Getenv("S3_VIDEO_BUCKET_NAME")
	maxChunkSize := config.MAX_CHUNK_SIZE

	firstReq, err := stream.Recv()
	if err != nil {
		return fmt.Errorf("error receiving initial request: %v", err)
	}

	objectKey := firstReq.FileName

	createResp, err := svc.CreateMultipartUpload(context.TODO(), &s3.CreateMultipartUploadInput{
		Bucket:      aws.String(bucketName),
		Key:         aws.String(objectKey),
		ContentType: aws.String("video/mp4"),
	})
	if err != nil {
		return fmt.Errorf("unable to create multipart upload: %v", err)
	}

	uploadID := createResp.UploadId
	fmt.Println("Upload ID:", *uploadID)

	var parts []types.CompletedPart
	partNumber := int32(1)

	for {
		req, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("error receiving chunk: %v", err)
		}

		if len(req.Chunk) > int(maxChunkSize) {
			return fmt.Errorf("chunk size exceeds maximum allowed size of %d bytes", maxChunkSize)
		}

		uploadResp, err := svc.UploadPart(context.TODO(), &s3.UploadPartInput{
			Bucket:     aws.String(bucketName),
			Key:        aws.String(objectKey),
			PartNumber: aws.Int32(partNumber),
			UploadId:   uploadID,
			Body:       bytes.NewReader(req.Chunk),
		})
		if err != nil {
			_, abortErr := svc.AbortMultipartUpload(context.TODO(), &s3.AbortMultipartUploadInput{
				Bucket:   aws.String(bucketName),
				Key:      aws.String(objectKey),
				UploadId: uploadID,
			})
			if abortErr != nil {
				return fmt.Errorf("unable to abort multipart upload: %v", abortErr)
			}
			return fmt.Errorf("unable to upload part %d: %v", partNumber, err)
		}

		parts = append(parts, types.CompletedPart{
			ETag:       uploadResp.ETag,
			PartNumber: aws.Int32(partNumber),
		})

		fmt.Printf("Uploaded part %d\n", partNumber)
		partNumber++
	}

	_, err = svc.CompleteMultipartUpload(context.TODO(), &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(bucketName),
		Key:      aws.String(objectKey),
		UploadId: uploadID,
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: parts,
		},
	})
	if err != nil {
		_, abortErr := svc.AbortMultipartUpload(context.TODO(), &s3.AbortMultipartUploadInput{
			Bucket:   aws.String(bucketName),
			Key:      aws.String(objectKey),
			UploadId: uploadID,
		})
		if abortErr != nil {
			return fmt.Errorf("unable to abort multipart upload: %v", abortErr)
		}
		return fmt.Errorf("unable to complete multipart upload: %v", err)
	}

	fmt.Println("Successfully uploaded file")
	return stream.SendAndClose(&proto.VideoUploadResponse{Status: "success"})
}

func main() {
	lis, err := net.Listen("tcp", os.Getenv("APP_PORT"))
	if err != nil {
		logger.Logger.Warn("error to listen:", zap.Error(err))
		return
	}

	s := grpc.NewServer(grpc.MaxRecvMsgSize(10 * 1024 * 1024 * 1024)) // 10GB max message size
	proto.RegisterVideoUploadServiceServer(s, &server{})

	reflection.Register(s)
	logger.Logger.Info("VIDEO SERVER Started successfully !!!")

	if err := s.Serve(lis); err != nil {
		logger.Logger.Warn("error to serve:", zap.Error(err))
	}
}

// func CreatePreSignedURL(bucketName, objectKey string) (string, error) {
// 	svc := awsClient.S3_CLIENT
// 	presignClient := s3.NewPresignClient(svc)

// 	req, err := presignClient.PresignGetObject(context.TODO(), &s3.GetObjectInput{
// 		Bucket: aws.String(bucketName),
// 		Key:    aws.String(objectKey),
// 	}, s3.WithPresignExpires(4*time.Hour))
// 	if err != nil {
// 		return "", fmt.Errorf("failed to create presigned URL: %v", err)
// 	}

// 	return req.URL, nil
// }

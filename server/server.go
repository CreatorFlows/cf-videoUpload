package main

import (
	"bytes"
	"context"
	"errors"
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
	logger.InitLogger()

	if err := awsClient.AWSClient(awsClient.AWS_CONFIG{
		ACCESS_KEY: os.Getenv("ACCESS_KEY"),
		SECRET_KEY: os.Getenv("SECRET_KEY"),
		REGION:     os.Getenv("REGION"),
	}); err != nil {
		logger.Logger.Error("error in initiating aws client", zap.Error(err))
		os.Exit(1)
	}
	if err := awsClient.S3Client(); err != nil {
		logger.Logger.Error("error in initiating S3 client", zap.Error(err))
		os.Exit(1)
	}
	logger.Logger.Info("aws config initialized", zap.Any("AWS_CONFIG", map[string]string{
		"ACCESS_KEY": os.Getenv("ACCESS_KEY"),
		"SECRET_KEY": os.Getenv("SECRET_KEY"),
	}))
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
		return errors.New("error receiving initial request")
	}

	objectKey := firstReq.FileName

	createResp, err := svc.CreateMultipartUpload(context.TODO(), &s3.CreateMultipartUploadInput{
		Bucket:      aws.String(bucketName),
		Key:         aws.String(objectKey),
		ContentType: aws.String("video/mp4"),
	})
	if err != nil {
		return err
	}

	uploadID := createResp.UploadId
	logger.Logger.Info("UploadID", zap.String("UploadID", *uploadID))

	var parts []types.CompletedPart
	partNumber := int32(1)

	for {
		req, err := stream.Recv()
		if err == io.EOF {
			logger.Logger.Info("end of the file")
			break
		}
		if err != nil {
			logger.Logger.Warn("error in receiving chunks", zap.Error(err))
			return errors.New("error receiving chunk")
		}

		if len(req.Chunk) > int(maxChunkSize) {
			logger.Logger.Warn("chunk size exceeded", zap.Error(errors.New("exceeds maximum allowed size of bytes")))
			return errors.New("chunk size exceeds maximum allowed size of bytes")
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
				logger.Logger.Warn("unable to abort multipart", zap.Error(abortErr))
				return errors.New("unable to abort multipart upload")
			}
			logger.Logger.Warn("unable to upload part", zap.Error(err))
			return errors.New("unable to upload part")
		}
		logger.Logger.Info("uploading part", zap.Int32("part_number", partNumber), zap.String("etag", *uploadResp.ETag))

		parts = append(parts, types.CompletedPart{
			ETag:       uploadResp.ETag,
			PartNumber: aws.Int32(partNumber),
		})

		logger.Logger.Info("part uploaded", zap.String("partNumber:", string(partNumber)))
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
			logger.Logger.Warn("unable to abort multipart upload", zap.Error(abortErr))
			return errors.New("unable to abort multipart upload")
		}
		logger.Logger.Warn("unable to complete multipart upload", zap.Error(abortErr))
		return errors.New("unable to complete multipart upload")
	}
	fileURL := fmt.Sprintf("https://%s.s3.%s.amazonaws.com/%s", bucketName, os.Getenv("REGION"), objectKey)

	logger.Logger.Info("Successfully uploaded file")
	return stream.SendAndClose(&proto.VideoUploadResponse{Status: "success", Url: fileURL, FileName: objectKey})
}

func main() {
	lis, err := net.Listen("tcp", os.Getenv("APP_PORT"))
	if err != nil {
		logger.Logger.Error("error to listen:", zap.Error(err))
		return
	}
	logger.Logger.Info("TCP started", zap.String("PORT", os.Getenv("APP_PORT")))

	s := grpc.NewServer(grpc.MaxRecvMsgSize(10 * 1024 * 1024 * 1024))
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
// 		return "", errors.New("failed to create presigned URL: %v", err)
// 	}

// 	return req.URL, nil
// }

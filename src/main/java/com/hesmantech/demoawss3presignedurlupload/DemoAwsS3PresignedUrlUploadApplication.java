package com.hesmantech.demoawss3presignedurlupload;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.PresignedPutObjectRequest;
import software.amazon.awssdk.services.s3.presigner.model.PutObjectPresignRequest;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/api")
@SpringBootApplication
public class DemoAwsS3PresignedUrlUploadApplication implements CommandLineRunner {

	private static final Logger logger = LoggerFactory.getLogger(DemoAwsS3PresignedUrlUploadApplication.class);
	private static final String BUCKET_NAME = "bao.upload-file-us-east-1";
	private static final Region REGION = Region.US_EAST_1;

	public static void main(String[] args) {
		SpringApplication.run(DemoAwsS3PresignedUrlUploadApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {
//		String keyName = "pre-singed-url-upload-example.jpg";
//		AwsBasicCredentials awsCreds = AwsBasicCredentials.create(
//				"AKIA3ADNNN4FKXECQEHI",
//				"shW+hZf/X+tyWfaCtyKLD+nbcZmtUQIxv18EtWb6");
//		S3Presigner presigner = S3Presigner.builder()
//				.credentialsProvider(StaticCredentialsProvider.create(awsCreds))
//				.region(REGION)
//				.build();

//		signBucket(presigner, bucketName, keyName);
//		presigner.close();
	}

//	void signBucket(S3Presigner presigner, String bucketName, String keyName) {
//		try {
//			signBucket(presigner, bucketName, keyName, new FileInputStream("src/main/resources/3013065.jpg"));
//		} catch (FileNotFoundException e) {
//			logger.error("File not found: {}", e.getMessage());
//			e.printStackTrace();
//		}
//	}

	PresignedPutObjectRequest getPresignedPutObjectRequest(S3Presigner presigner, String bucketName, String keyName) {
		PutObjectRequest objectRequest = PutObjectRequest.builder()
				.bucket(bucketName)
				.key(keyName)
				.contentType("image/jpeg")
				.build();

		PutObjectPresignRequest presignRequest = PutObjectPresignRequest.builder()
				.signatureDuration(Duration.ofMinutes(10))
				.putObjectRequest(objectRequest)
				.build();

		return presigner.presignPutObject(presignRequest);
	}

	void signBucket(S3Presigner presigner, String bucketName, String keyName, InputStream fileInputStream) {
		try {
			PresignedPutObjectRequest presignedRequest = getPresignedPutObjectRequest(presigner, bucketName, keyName);

			URL url = presignedRequest.url();
			logger.info("Presigned URL to upload a file to: " + url.toString());
			logger.info("Which HTTP method needs to be used when uploading a file: " +
					presignedRequest.httpRequest().method());

			HttpURLConnection connection = (HttpURLConnection) url.openConnection();
			connection.setDoOutput(true);
			connection.setRequestProperty("Content-Type", "image/jpeg");
			connection.setRequestMethod("PUT");
			OutputStream out = connection.getOutputStream();
			out.write(IOUtils.toByteArray(fileInputStream));
			out.close();

			logger.info("HTTP response code is {}", connection.getResponseCode());
		} catch (S3Exception e) {
			logger.error("Error occured durring upload from pre-signed URL to S3: {}", e.getMessage());
			e.printStackTrace();
		} catch (IOException e) {
			logger.error("Error occured durring connection with the upload URL: {}", e.getMessage());
			e.printStackTrace();
		}
	}

	@PostMapping("/upload")
	public ResponseEntity<?> upload(@RequestParam MultipartFile file, @RequestParam String fileName) {
		Map<String, Object> body = new HashMap<>();
		HttpStatus status = HttpStatus.OK;
		try (S3Presigner presigner = S3Presigner.builder()
				.credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(
						"AKIA3ADNNN4FKXECQEHI",
						"shW+hZf/X+tyWfaCtyKLD+nbcZmtUQIxv18EtWb6")))
				.region(REGION)
				.build()) {

			signBucket(presigner, BUCKET_NAME, fileName, file.getInputStream());
			body.put("message", "Upload successfully!");
		} catch (IOException e) {
			logger.error("Error: {}", e.getMessage());
			e.printStackTrace();

			body.put("message", "Failed to upload.");
			body.put("error", e.getMessage());
			status = HttpStatus.BAD_REQUEST;
		}
		return new ResponseEntity<>(body, status);
	}

	@GetMapping("/get-pre-signed-url")
	public ResponseEntity<?> getPreSignedUrl(@RequestParam String fileName) {
		Map<String, Object> body = new HashMap<>();
		HttpStatus status = HttpStatus.OK;
		try (S3Presigner presigner = S3Presigner.builder()
				.credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(
						"AKIA3ADNNN4FKXECQEHI",
						"shW+hZf/X+tyWfaCtyKLD+nbcZmtUQIxv18EtWb6")))
				.region(REGION)
				.build()) {

			PresignedPutObjectRequest presignedRequest = getPresignedPutObjectRequest(presigner, BUCKET_NAME, fileName);
			body.put("message", "Get pre-signed URL successfully!");
			body.put("presignedUrl", presignedRequest.url().toString());
			body.put("method", presignedRequest.httpRequest().method().toString());
		} catch (Exception e) {
			logger.error("Error: {}", e.getMessage());
			e.printStackTrace();

			body.put("message", "Failed to get pre-signed URL.");
			body.put("error", e.getMessage());
			status = HttpStatus.BAD_REQUEST;
		}
		return new ResponseEntity<>(body, status);
	}
}

import { S3Client, PutObjectCommand, GetObjectCommand } from "@aws-sdk/client-s3";
import { SESClient, SendEmailCommand } from "@aws-sdk/client-ses";
import sharp from 'sharp';
import path from "path";

const s3Client = new S3Client({})
const sesClient = new SESClient({});

/*
 * This function is used to convert data from stream to buffer
 */
const streamToBuffer = async (stream) => {
    return await new Promise((resolve, reject) => {
        const chunks = [];
        stream.on('data', (chunk) => chunks.push(chunk));
        stream.on('error', reject);
        stream.on('end', () => resolve(Buffer.concat(chunks)));
    });
}

/* 
 * This function is used to get the S3 Object details
 */
const getS3ObjectDetails = async ({ bucket, key }) => {
    try {
        // Generate command then execute and send
        const command = new GetObjectCommand({
            Bucket: bucket,
            Key: key
        });

        const response = await s3Client.send(command);

        return response;
    } catch (e) {
        console.info("Something went wrong while fetching S3 Object", e);
        return null;
    }
}

/* 
 * This function is used to upload the S3 Object
 */
const putS3Object = async ({ bucket, key, body }) => {
    try {
        // Generate command then execute and send
        const command = new PutObjectCommand({
            Bucket: bucket,
            Key: key,
            Body: body
        });

        const response = await s3Client.send(command);

        return response;

    } catch (e) {
        console.info("Something went wrong while uploading S3 Object", e);
        return null;
    }
}

/**
 * 
 * This function is the main function, execution starts from here
 */
export const handler = async (event) => {
    for (const record of event.Records) {
        // Fetch bucket, key of the object from the record
        const bucket = record.s3.bucket.name;
        const key = record.s3.object.key;

        // Fetch the S3 Object details
        let s3ObjectDetails = null;
        s3ObjectDetails = await getS3ObjectDetails({ bucket, key });

        // If the object details found and file is of image type then create thumbnail for that image
        if (s3ObjectDetails && s3ObjectDetails.ContentType && s3ObjectDetails.ContentType.startsWith('image/') && ['.jpg', '.jpeg', '.png'].some(ext => key.toLowerCase().endsWith(ext))) {
            console.info("***********************");
            console.info("This is image type file", bucket, key);
            console.info("***********************");
            try {
                await generateAndUploadThumbnail({ bucket, key, s3ObjectDetails });
            }
            catch (e) {
                console.error("Something went wrong while genrating thumbnail and save it for key", key);
            }
        }
    }

    console.log(`Successfully processed S3 event(s).`);

    const response = {
        statusCode: 200,
        body: JSON.stringify('S3 Event processed successfully.'),
    };
    return response;
};

/*
 * This function is used to generate the image thumbnail
 * It will resize image (default 200 X 200) and format to jpeg and create buffer data 
 */
const generateImageThumbnail = async ({ imageBufferData, height = 200, weight = 200 }) => {
    try {
        return await sharp(imageBufferData).resize(height, weight).png().toBuffer();
    }
    catch (e) {
        console.error("Something went wrong while genrating image thumbnail", e);
    }
}


/*
 * This function is used to generate thumbnail image and then upload in different folder in S3 Bucket
 */
const generateAndUploadThumbnail = async ({ bucket, key, s3ObjectDetails }) => {
    try {
        // Here, it wil be 2 elements array where first one contains folder name and second one contains name of the file
        const keysSplit = key.split("/");

        // Fetch filenae without extension as we have png as thumbnail image format then generate thumbnail key
        const fileNameWithoutExtension = path.parse(keysSplit[1]).name;

        const thumbnailKey = `image-thumbnails/${fileNameWithoutExtension}_thumb.png`;

        // Get stream data of S3 Object
        const bodyStream = s3ObjectDetails.Body;

        // Convert stream data to buffer
        const bodyBuffer = await streamToBuffer(bodyStream);

        // Generate thumbnail
        const imageBuffer = await generateImageThumbnail({
            imageBufferData: bodyBuffer
        });

        if (imageBuffer) {
            // Put generated thumbnail 
            const response = await putS3Object({
                bucket,
                key: thumbnailKey,
                body: imageBuffer
            });

            if (!response) {
                throw new Error("Something went wrong while uploading image thumbnail")
            }
        }
        else {
            throw new Error("Something went wrong while generating image thumbnail")
        }
    }
    catch (err) {
        console.info("Something went wrong while generating & uploading thumbnail", err);
    }
}
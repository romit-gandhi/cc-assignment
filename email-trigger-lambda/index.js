import { S3Client, ListObjectsCommand, HeadObjectCommand } from "@aws-sdk/client-s3";
import { SESClient, SendEmailCommand } from "@aws-sdk/client-ses";
import dayjs from "dayjs";

const s3Client = new S3Client({})
const sesClient = new SESClient({});

/* 
 * This function is used to get the S3 Object details
 */
const listSpecificDateS3Objects = async ({ bucket, prefix, objects = [], marker }) => {
    try {
        // Preapare params, if marker is provided then set Marker in params
        const params = {
            Bucket: bucket,
            Prefix: prefix,
            MaxKeys: 1000
        };

        if (marker) {
            params.Marker = marker;
        }

        //  Get command and execute it
        const command = new ListObjectsCommand(params);

        const response = await s3Client.send(command);

        //  Loop through all contents and if key is not of folder's then push that record in objects
        response.Contents.forEach(content => {
            if (content.Key !== `${prefix}/`) {
                objects.push(content);
            }
        });


        if (response.NextMarker) {
            listSpecificDateS3Objects({
                bucket,
                prefix,
                objects,
                marker: response.NextMarker

            })
        }
        return objects;
    } catch (e) {
        console.info("Something went wrong while fetching S3 Object", e);
        return null;
    }
}

/* 
 * This function is used to get the S3 Object Metadata
 */
const getS3ObjectMetadata = async ({ bucket, key }) => {
    try {
        // Generate command then execute and send
        const command = new HeadObjectCommand({
            Bucket: bucket,
            Key: key
        });

        const response = await s3Client.send(command);

        return response;
    } catch (e) {
        console.info("Something went wrong while fetching S3 Object Metadata", e);
        return null;
    }
}

const sendEmail = async ({
    from,
    to,
    subject,
    body
}) => {
    try {
        // Prepare params for email
        const params = {
            Destination: {
                ToAddresses: [to]
            },
            Message: {
                Body: {
                    Html: {
                        Data: body
                    }
                },
                Subject: {
                    Data: subject
                }
            },
            Source: from
        };

        // Trigger email send
        const command = new SendEmailCommand(params);
        const data = await sesClient.send(command);
        return data;
    }
    catch (err) {
        console.info("Someting went wrong while sending an email", err);
    }
};

const genrateHTMLForEmail = (objects) => {
    let objectsString = ``;
     objects.forEach(object => {
        objectsString += `
            <tr>
                <td>${object.s3Uri}</td>
                <td>${object.fileName}</td>
                <td>${object.contentType}</td>
                <td>${object.size}</td>
            </tr>
        `;
    })
    return `
    <html>
        <body>
            <table cellpadding="0" cellspacing="0" width="640" align="center" border="1">
                <tr>
                    <th>S3 URI</th>
                    <th>Object Name</th>
                    <th>Content Type</th>
                    <th>Size (Bytes)</th>
                </tr>
                ${objectsString}
                
            </table>
        </body>
    </<html>
    `;
}

/**
 * 
 * This function is the main function, execution starts from here
 */
export const handler = async (event) => {
    // Specify bucket and key
    const bucket = "2022mt93600-cc-assignment1";
    const key = "input-files";

    // Define yesterday and today's date
    const yesterday = dayjs().add(1, 'day').subtract(1, 'day').format("YYYY-MM-DD")
    const today = dayjs().add(1, 'day').format("YYYY-MM-DD");

    // Fetch all S3 Objects
    const objects = await listSpecificDateS3Objects({ bucket, prefix: key });
    const todaysObjects = [];

    // Loop through all objects
    for (const object of objects) {
        // Check that the last modified date of that object is yersterday's or not
        const date = dayjs(object.LastModified);
        const isTodaysObject = date.isAfter(yesterday) && date.isBefore(today);

        //  If it is then procceed
        if (isTodaysObject) {
            // Fetch object metadata
            const objectMetadata = await getS3ObjectMetadata({
                bucket,
                key: object.Key
            });

            // Prepare data
            const todayObjectData = {
                s3Uri: `s3://${bucket}/${object.Key}`,
                fileName: object.Key.split("/")[1],
                size: object.Size,
                contentType: ""
            }

            //  If object metadata found then update the contentType
            if (objectMetadata) {
                todayObjectData.contentType = objectMetadata.ContentType;
            }

            // Push that in todaysObjects
            todaysObjects.push(todayObjectData)
        }
    }

    const htmlBody = genrateHTMLForEmail(todaysObjects);

    await sendEmail({
        from: "2022mt93600@wilp.bits-pilani.ac.in",
        to: "2022mt93600@wilp.bits-pilani.ac.in",
        subject: `Summary of objects added in ${bucket} on ${yesterday}`,
        body: htmlBody
    });

    const response = {
        statusCode: 200,
        body: JSON.stringify('Summary generated and email sent.'),
    };
    return response;
};


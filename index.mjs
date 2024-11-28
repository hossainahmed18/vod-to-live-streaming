import { spawnSync } from 'child_process';
import { mkdirSync } from 'fs';
import path from 'path';
import fs from 'fs';
import { S3Client, PutObjectCommand } from '@aws-sdk/client-s3';

const destinationBucket = '';
const ffmpegPath = 'ffmpeg';

/*
export const handler = async (event) => {
    console.log(JSON.stringify(event, null, 2));
    try {
        if (event.Input?.inputHLS) {
            mkdirSync(localDirectory, { recursive: true });
            await convertHlsToMp3({ fileUri: event.Input.inputHLS });
        }
    } catch (error) {
        console.error('Error', error);
    }
};

const convertHlsToMp3 = async ({ fileUri }) => {
    const fileNameWithoutExtension = path.parse(fileUri).name;
    const tempFolderPath = `${localDirectory}/converted`;
    mkdirSync(tempFolderPath, { recursive: true });
    const tempFilePath = `${tempFolderPath}/${fileNameWithoutExtension}.mp3`;
    try {
        const ffmpegCommand = [
            '-y',         
            '-i', fileUri, 
            '-vn',       
            '-acodec', 'libmp3lame', 
            '-q:a', '0',  
            tempFilePath 
        ];
        const result = spawnSync(ffmpegPath, ffmpegCommand, { stdio: 'inherit' });

        if (result.error) {
            console.error('FFmpeg Error:', result.error.message);
        }
        console.log('Conversion completed:', tempFilePath);
    } catch (error) {
        console.error('Error during conversion:', error.message);
    }
};
*/

const downloadManifestAndSegments = async (manifestUrl, downloadDir) => {
    try {
        fs.mkdirSync(downloadDir, { recursive: true });
        const manifestResponse = await fetch(manifestUrl);
        if (!manifestResponse.ok) {
            throw new Error(`Failed to download manifest: ${manifestResponse.statusText}`);
        }
        const manifestContent = await manifestResponse.text();
        const manifestPath = path.join(downloadDir, path.basename(manifestUrl));
        fs.writeFileSync(manifestPath, manifestContent);
        const lines = manifestContent.split('\n');
        const segments = lines.filter(line => line && !line.startsWith('#'));

        for (const segment of segments) {
            const segmentUrl = new URL(segment, manifestUrl).href;
            const segmentName = path.basename(segment);
            const segmentPath = path.join(downloadDir, segmentName);

            console.log(`Downloading segment: ${segmentUrl}`);
            const segmentResponse = await fetch(segmentUrl);

            if (!segmentResponse.ok) {
                throw new Error(`Failed to download segment: ${segmentUrl}`);
            }
            const arrayBuffer = await segmentResponse.arrayBuffer();
            const segmentData = Buffer.from(arrayBuffer);
            fs.writeFileSync(segmentPath, segmentData);
        }
        return { manifestPath, segments };
    } catch (error) {
        console.error('Error downloading HLS manifest and segments:', error);
        throw error;
    }
};

const uploadToS3 = async (uploadDir) => {
    try {
        const s3Client = new S3Client({ region: 'eu-north-1' });
        const files = fs.readdirSync(uploadDir);

        for (const file of files) {
            const filePath = path.join(uploadDir, file);
            const s3Key = `${uploadDir}/${file}`;

            console.log(`Uploading ${filePath} to s3://${destinationBucket}/${s3Key}`);
            const fileContent = fs.readFileSync(filePath);

            const command = new PutObjectCommand({
                Bucket: destinationBucket,
                Key: s3Key,
                Body: fileContent,
            });

            await s3Client.send(command);
            console.log(`Uploaded: ${filePath}`);
        }

        console.log('All files uploaded successfully!');
    } catch (error) {
        console.error('Error uploading to S3:', error);
        throw error;
    }
};
const extractPathSegment = (url) => {
    const parsedUrl = new URL(url);
    const extractedSegment = path.dirname(parsedUrl.pathname);
    return extractedSegment.startsWith('/') ? extractedSegment.substring(1) : extractedSegment;
};
export const handler = async (event) => {
    console.log(JSON.stringify(event, null, 2));
    try {
        if (event.Input?.inputHLS?.length > 0) {
            const localDirectory = extractPathSegment(event.Input.inputHLS);
            await downloadManifestAndSegments(event.Input.inputHLS, localDirectory);
            await uploadToS3(localDirectory);
        }
    } catch (error) {
        console.error('Error', error);
    }
}
(async () => {
    await handler({
        Input: {
            inputHLS: '',
        },
    });
})();

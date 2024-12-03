import { spawnSync } from 'child_process';
import { mkdirSync } from 'fs';
import path from 'path';
import fs from 'fs';
import { S3Client, PutObjectCommand } from '@aws-sdk/client-s3';
import axios from 'axios';

const destinationBucket = '';
const s3Client = new S3Client({ region: 'eu-north-1' });
const ffmpegPath = 'ffmpeg';
const mediaPackageIngestUrl = '';
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
        const mainManifestContent = await manifestResponse.text();
        const mainManifestPath = path.join(downloadDir, path.basename(manifestUrl));
        fs.writeFileSync(mainManifestPath, mainManifestContent);
        const childManifestFiles = mainManifestContent.split('\n').filter(line => line && !line.startsWith('#'));

        for (const childManifestFile of childManifestFiles) {
            const childManifestUrl = new URL(childManifestFile, manifestUrl.replace(path.basename(manifestUrl), '')).href;
            const childManifestResponse = await fetch(childManifestUrl);
            if (!childManifestResponse.ok) {
                throw new Error(`Failed to download child manifest: ${childManifestUrl}`);
            }
            const childManifestContent = await childManifestResponse.text();
            const childManifestPath = path.join(downloadDir, path.basename(childManifestUrl));
            fs.writeFileSync(childManifestPath, childManifestContent);
            const segmentFiles = childManifestContent.split('\n').filter(line => line && !line.startsWith('#'));
            for (const segment of segmentFiles) {
                const segmentUrl = new URL(segment, manifestUrl).href;
                const segmentName = path.basename(segment);
                console.log(`Downloading segment: ${segmentUrl}`);
                const segmentResponse = await fetch(segmentUrl);

                if (!segmentResponse.ok) {
                    throw new Error(`Failed to download segment: ${segmentUrl}`);
                }
                const arrayBuffer = await segmentResponse.arrayBuffer();
                const segmentData = Buffer.from(arrayBuffer);
                fs.writeFileSync(path.join(downloadDir, segmentName), segmentData);
            }
        }
    } catch (error) {
        console.error('Error downloading HLS manifest and segments:', error);
        throw error;
    }
};
/*
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
*/

const uploadManifestSegmentsToS3 = async (fileKey, fileContent) => {
    try {
        const command = new PutObjectCommand({
            Bucket: destinationBucket,
            Key: fileKey,
            Body: fileContent,
        });
        await s3Client.send(command);
    } catch (error) {
        console.error('Error uploading to S3:', error);
        throw error;
    }
};

const ingestHlsToMediaPackage = async (manifestDir, manifestUrl) => {
    try {

        const mainManifestContent = fs.readFileSync(`${manifestDir}/${path.basename(manifestUrl)}`, 'utf8');
        const childManifestFiles = mainManifestContent.split('\n').filter(line => line && !line.startsWith('#'));
        for (const childManifestFile of childManifestFiles) {
            const childManifestFileContent = fs.readFileSync(path.join(manifestDir, childManifestFile), 'utf8');
            const segments = childManifestFileContent.split('\n').filter(line => line.endsWith('.ts'));
            console.log(`Found ${segments.length} segments in manifest.`);
            
            for (const segment of segments) {
                const segmentIngestUrl = mediaPackageIngestUrl.replace('index', segment);
                console.log(`Uploading segment to: ${segmentIngestUrl}`);
                const segmentData = fs.readFileSync(path.join(manifestDir, segment));
                
                await axios.put(segmentIngestUrl, segmentData, {
                    headers: {
                        'Content-Type': 'video/MP2T',
                    },
                });
                
                //await uploadManifestSegmentsToS3(path.join(manifestDir, segment), segmentData);
                console.log(`Uploaded segment: ${segment}`);
            }
                
            
            console.log(`Uploaded all segments for manifest: ${childManifestFile}`);
            const childManifestIngestUrl = mediaPackageIngestUrl.replace('index', childManifestFile);
            console.log(`Uploading child manifest to: ${childManifestIngestUrl}`);
            await axios.put(childManifestIngestUrl, childManifestFileContent, {
                headers: {
                    'Content-Type': 'application/vnd.apple.mpegurl',
                },
            });
            
            //await uploadManifestSegmentsToS3(path.join(manifestDir, childManifestFile), childManifestFileContent);
            console.log(`Uploaded child manifest: ${childManifestIngestUrl}`);
        }
        
        const mainManifestIngestUrl = `${mediaPackageIngestUrl}.m3u8`
        console.log(`Uploading main manifest: ${mainManifestIngestUrl}`);
        await axios.put(mainManifestIngestUrl, mainManifestContent, {
            headers: {
                'Content-Type': 'application/vnd.apple.mpegurl',
            },
        });
        console.log('Main Manifest uploaded successfully.');
    } catch (error) {
        console.error('Error during HLS ingestion:', error.message);
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
            await ingestHlsToMediaPackage(localDirectory, event.Input.inputHLS);
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

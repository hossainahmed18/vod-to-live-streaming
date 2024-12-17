import { spawnSync } from 'child_process';
import { mkdirSync } from 'fs';
import path from 'path';
import fs from 'fs';
import { S3Client, PutObjectCommand } from '@aws-sdk/client-s3';
import axios from 'axios';

const destinationBucket = 'eu-north-1-stage-video-stest-mc-output';
const s3Client = new S3Client({ region: 'eu-north-1' });
const ffmpegPath = 'ffmpeg';

const mediaPackageIngestUrl = '';
let currentSequenceNo = 660; // (any number) used start time over window in mediapackage 60s
const segmentDuration = '6.00000';
const childManifestFile = 'index1080p.m3u8';
const segmentNamePrefix = 'index1080p_hls';

const streamConfigs = [
    {
        fileName: 'index1080p.m3u8',
        bandwidth: 5666069,
        avgBandwidth: 5271919,
        codecs: 'avc1.4d4028,mp4a.40.2',
        resolution: '1920x1080',
        frameRate: 30.000,
    }
];


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

const generateHLSChildManifest = (segments) => {
    const programDateTime = new Date().toISOString();
    let manifest = "#EXTM3U\n";
    manifest += "#EXT-X-VERSION:3\n";
    manifest += `#EXT-X-TARGETDURATION:${Math.round(Number(segmentDuration))}\n`;
    manifest += `#EXT-X-MEDIA-SEQUENCE:${currentSequenceNo}\n`;
    manifest += `#EXT-X-PROGRAM-DATE-TIME:${programDateTime}\n`;
    segments.forEach(segment => {
        manifest += `#EXTINF:${segmentDuration},\n`;
        manifest += `${segment}\n`;
    });
    return manifest;
}

const generateMainManifest = (streams) => {

    let manifest = "#EXTM3U\n";
    manifest += "#EXT-X-VERSION:3\n";
    manifest += "#EXT-X-INDEPENDENT-SEGMENTS\n";

    streams.forEach(stream => {
        manifest += `#EXT-X-STREAM-INF:BANDWIDTH=${stream.bandwidth},AVERAGE-BANDWIDTH=${stream.avgBandwidth},CODECS="${stream.codecs}",RESOLUTION=${stream.resolution},FRAME-RATE=${stream.frameRate.toFixed(3)}\n`;
        manifest += `${stream.fileName}\n`;
    });

    return manifest;
}

const ingestHlsToMediaPackage = async (manifestDir) => {
    try {
        const segments = [
            '1.ts',
            '2.ts',
            '3.ts',
            '4.ts',
            '5.ts',
            '6.ts',
            '7.ts',
            '8.ts',
            '9.ts',
            '10.ts'
        ];
        const newSegmentList = [];
        for (const [index, segment] of segments.entries()) {
            const newSegmentName = `${segmentNamePrefix}_00${currentSequenceNo + index}.ts`;
            newSegmentList.push(newSegmentName);

            const segmentIngestUrl = mediaPackageIngestUrl.replace('index', newSegmentName);
            await axios.put(segmentIngestUrl, fs.readFileSync(path.join(manifestDir, segment)), {
                headers: {
                    'Content-Type': 'video/MP2T',
                },
            });
            console.log(`Uploading segment to: ${segmentIngestUrl}`);
        }
        const childManifestIngestUrl = mediaPackageIngestUrl.replace('index', childManifestFile);
        await axios.put(childManifestIngestUrl, generateHLSChildManifest(newSegmentList), {
            headers: {
                'Content-Type': 'application/vnd.apple.mpegurl',
            },
        });
        console.log(`Uploaded child manifest: ${childManifestIngestUrl}`);
        const mainManifestIngestUrl = `${mediaPackageIngestUrl}.m3u8`;
        await axios.put(mainManifestIngestUrl, generateMainManifest(streamConfigs), {
            headers: {
                'Content-Type': 'application/vnd.apple.mpegurl',
            },
        });
        console.log(`Uploading main manifest to: ${mainManifestIngestUrl}`);
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
            //await downloadManifestAndSegments(event.Input.inputHLS, localDirectory);
            await ingestHlsToMediaPackage(localDirectory);
        }
    } catch (error) {
        console.error('Error', error);
    }
}
(async () => {
    await handler({
        Input: {
            inputHLS: 'https://vimond.video-output.eu-north-1-dev.vmnd.tv/e62aa4eb-6bf7-4087-8cbd-4d2f2819a419/hls/index.m3u8',
        },
    });
})();

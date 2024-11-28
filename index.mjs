import { spawnSync } from 'child_process';
import { mkdirSync } from 'fs';
import path from 'path';

const localDirectory = 'tmp';
const ffmpegPath = 'ffmpeg';

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

(async () => {
    console.log('Uploading video to S3...');
    const result = await handler({ Input: { inputHLS: 'https://test-streams.mux.dev/x36xhzz/x36xhzz.m3u8' } });
    console.log(JSON.stringify(result, null, 2));
})()
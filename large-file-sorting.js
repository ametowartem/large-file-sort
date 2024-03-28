const fs = require('fs');
const readline = require('readline');

const originFilePath = 'large-file.txt';
const sortedLargeFileName = 'sorted-large-file.txt';
const tempFileName = 'temp_file_';

const tempDirName = 'temp_';

const blockSize = 2 ** 24;

const tempFilesDir = fs.mkdtempSync(tempDirName) + '/';

async function fileReader(filePath, blockSize) {
    return new Promise((resolve, reject) => {
        let index = 0;

        const input = fs.createReadStream(filePath, {highWaterMark: blockSize, encoding: 'utf8'});

        input.on('data', (data) => {
            const str = data.toString();

            data = str.split('\n')
                .sort((a, b) => a.localeCompare(b))
                .join('\n');

            fs.writeFileSync(`${tempFilesDir}${tempFileName}${++index}.txt`, data, {flag: "w+"});
        })

        input.on('close', () => {
            // console.log(process.memoryUsage());

            resolve(true);
        })

        input.on('error', (err) => {
            console.log(err);

            reject(err);
        })
    })
}

async function mergeSortingFiles(inputFiles, outputFile) {
    return new Promise((resolve, reject) => {
        const streams = inputFiles.map(inputFile => {
            return readline.createInterface({
                input: fs.createReadStream(inputFile, { highWaterMark: Math.floor(blockSize / inputFile.length) }),
            });
        });

        const mergeHeap = [];
        let remainingStreams = streams.length;

        function addToMergeHeap(stream, fileIndex) {
            function onLineHandler(line) {
                mergeHeap.push({ line, fileIndex });
                if (mergeHeap.length === 1) {
                    writeMinimalHeapElement();
                    stream.removeListener('line', onLineHandler);
                    stream.removeListener('close', onCloseHandler);
                    stream.removeListener('error', onErrorHandler);
                }
            }

            function onCloseHandler() {
                remainingStreams--;

                if (!remainingStreams) {
                    // console.log(process.memoryUsage())

                    for (let i = 0; i < inputFiles.length; i++) {
                        fs.rmSync(inputFiles.at(i));
                    }

                    resolve(true)
                }
            }

            function onErrorHandler(err) {
                reject(err)
            }

            stream.once('line', onLineHandler);
            stream.once('close', onCloseHandler);
            stream.once('error', onErrorHandler);
        }

        function writeMinimalHeapElement() {
            if (!mergeHeap.length) {
                return;
            }

            mergeHeap
                .sort((a, b) => a.line.localeCompare(b.line));
            const { line, fileIndex } = mergeHeap.shift();
            fs.writeFileSync(outputFile, line + '\n', { flag: 'a+' });

            const correspondingStream = streams[fileIndex];
            addToMergeHeap(correspondingStream, fileIndex);
        }

        for (let i = 0; i < streams.length; i++) {
            addToMergeHeap(streams[i], i);
        }
    })
}

async function mergeTempFilesToLargeFile(largeFileName) {
    const numberOfFilesStreams = 5;
    let iteration = 0;

    await fileReader(originFilePath, blockSize);

    while (true) {
        ++iteration;

        let files =  fs.readdirSync(tempFilesDir);
        files = files.filter((el) => el.includes(tempFileName)).map((el) => tempFilesDir + el);

        if (files.length === 1) {
            fs.copyFileSync(files.at(0), largeFileName)
            fs.rmSync(files.at(0))

            break;
        }

        if (files.length <= numberOfFilesStreams) {
            await mergeSortingFiles(files, largeFileName );

            break;
        }

        for (let i = 0; i <= files.length; i += numberOfFilesStreams) {
            await mergeSortingFiles(files.slice(i, i + numberOfFilesStreams), tempFilesDir + `${tempFileName}${iteration}_${i}.txt`);
        }
    }

    fs.rmdirSync(tempFilesDir);
}

mergeTempFilesToLargeFile(sortedLargeFileName)
    .then(() => {
        console.log('Successfully sorted and merged')
    })
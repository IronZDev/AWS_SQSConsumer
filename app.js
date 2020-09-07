const AWS = require('aws-sdk');
const { Consumer } = require('sqs-consumer');
const Jimp = require('jimp');

// Configure the region
AWS.config.update({
  accessKeyId: process.env.AWS_ACCESS_KEY, // aws access id here
  secretAccessKey: process.env.AWS_SECRET, // aws secret access key here
  sessionToken: process.env.AWS_SESSION_TOKEN,
  region: 'us-east-1',
  signatureVersion: 'v4',
});

const options = {
  region: 'us-east-1', // same as your bucket
}
const S3 = new AWS.S3(options);

const queueUrl = "https://sqs.us-east-1.amazonaws.com/575075258561/images-queue";

async function transformImage(buffer, rotation, type) {
  let changedBuffer;
  await Jimp.read(buffer)
      .then(image => {
        image.rotate(90 * -rotation) // Counterclockwise
            .getBuffer(type, (err, buffer) => {
              if (err) {
                // console.log(err);
                app.emit('processing_error', err);
              }
              changedBuffer = buffer;
            })
        console.log("File transformed!");
      })
  return changedBuffer;
}

async function processMessage(message) {
  console.log("Message received!");
  console.log(message);
  let sqsMessage = JSON.parse(message.Body);
  console.log(sqsMessage);
  // Get image
  const imageObj = await (new Promise((resolve, reject) => {
      S3.getObject({
        Bucket: 'images-to-process-mstokfisz',
        Key: sqsMessage.key,
      }, (err, data) => {
        if (err) {
          // console.log(err);
          app.emit('processing_error', err);
        } else {
          resolve(data)
        }
      });
    }));
  const editedImage = await transformImage(imageObj.Body, sqsMessage.rotation, imageObj.ContentType);
  // Upload image
  await (new Promise((resolve, reject) => {
    S3.putObject({
      Bucket: 'transformed-images-mstokfisz',
      Key: sqsMessage.key,
      ACL: 'private',
      Body: editedImage,
      ContentType: imageObj.ContentType
    }, (err, data) => {
      if (err) {
        // console.log(err);
        app.emit('processing_error', err.message);
      } else {
        resolve(data)
      }
    });
  }));
  console.log("File uploaded!");
}

// Create our consumer
const app = Consumer.create({
  queueUrl: queueUrl,
  handleMessage: async (message) => {
    await processMessage(message);
  },
  sqs: new AWS.SQS()
});

app.on('error', (err) => {
  console.error(err.message);
  console.log("RESTART");
  app.stop();
  app.start();
});

app.on('processing_error', (err) => {
  console.error(err.message);
  console.log("RESTART");
  app.stop();
  app.start();
});

console.log('SQS Consumer is running');

app.start();
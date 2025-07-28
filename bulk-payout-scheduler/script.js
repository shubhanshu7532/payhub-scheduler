// const { MongoClient } = require('mongodb');
// const { Parser } = require('json2csv');
// const nodemailer = require('nodemailer');
// const fs = require('fs');
// const path = require('path');
// require('dotenv').config();

// async function fetchAndEmailData() {
//   const client = new MongoClient(process.env.MONGO_URI, { useUnifiedTopology: true });

//   try {
//     await client.connect();
//     const db = client.db(process.env.DB_NAME);
   
//     const collection = db.collection(process.env.COLLECTION_NAME);

//     const data = await collection.find({cron_status:"pending"}).toArray();
//     if (!data.length) {
//       console.log("No data found");
//       return;
//     }

//   const fields = Object.keys(data[0]);

// // Remove _id, replace underscores with spaces, and capitalize
// const capitalizedFields = fields
//   .filter(field => field !== '_id')
//   .map(field => ({
//     label: field.replace(/_/g, ' ').toUpperCase(),  // "source_account_number" → "SOURCE ACCOUNT NUMBER"
//     value: field
//   }));

// const parser = new Parser({ fields: capitalizedFields });
// const csv = parser.parse(data);

// const filePath = path.join(__dirname, 'data.csv');
// fs.writeFileSync(filePath, csv);
//      const transporter = nodemailer.createTransport({
//         host: 'mail.privateemail.com',
//         port: 465,  // Use 587 for TLS if you prefer
//         secure: true,  // true for SSL, false for TLS
//         auth: {
//           user: 'ops@payhub.link',  // Replace with your email address
//           pass: 'payhub123$'  // Replace with your email password
//         }
//       });
  

//     const info = await transporter.sendMail({
//       from: process.env.EMAIL_FROM,
//       to: process.env.EMAIL_TO,
//       subject: 'MongoDB Data Export',
//       text: 'CSV data from MongoDB attached.',
//       attachments: [{ filename: 'data.csv', path: filePath }],
//     });

//     console.log('Email sent:', info.response);
//   } catch (err) {
//     console.error(err);
//   } finally {
//     await client.close();
//   }
// }


const { MongoClient } = require('mongodb');
const { Parser } = require('json2csv');
const nodemailer = require('nodemailer');
const fs = require('fs');
const path = require('path');
require('dotenv').config();

const BATCH_SIZE = 500;

async function fetchAndEmailData() {
  const client = new MongoClient(process.env.MONGO_URI, { useUnifiedTopology: true });
  const filePath = path.join(__dirname, 'data.csv');


  try {
    await client.connect();
    const db = client.db(process.env.DB_NAME);
    const collection = db.collection(process.env.COLLECTION_NAME);

    const pendingCursor = collection.find({ cron_status: "pending" });
    const total = await pendingCursor.count();

    if (total === 0) {
      console.log("No pending records found.");
      return;
    }

    if (fs.existsSync(filePath)) fs.unlinkSync(filePath); // Clean old file

    let skip = 0;
    let isFirstBatch = true;
    let idsToUpdate = [];

    while (skip < total) {
      const batch = await collection.find({ cron_status: "pending" })
        .skip(skip)
        .limit(BATCH_SIZE)
        .toArray();

      if (!batch.length) break;

      // Collect _ids for updating later
      idsToUpdate.push(...batch.map(doc => doc._id));

      const fields = Object.keys(batch[0])
        .filter(field => field !== '_id')
        .map(field => ({
          label: field.replace(/_/g, ' ').toUpperCase(),
          value: field
        }));

      const parser = new Parser({ fields, header: isFirstBatch });
      const csv = parser.parse(batch);

      fs.appendFileSync(filePath, csv + '\n');
      skip += BATCH_SIZE;
      isFirstBatch = false;
    }

    // Send email
    const transporter = nodemailer.createTransport({
        host: 'mail.privateemail.com',
        port: 465,  // Use 587 for TLS if you prefer
        secure: true,  // true for SSL, false for TLS
        auth: {
          user: 'ops@payhub.link',  // Replace with your email address
          pass: 'payhub123$'  // Replace with your email password
        }
      });
    await transporter.sendMail({
      from: process.env.EMAIL_FROM,
      to: process.env.EMAIL_TO,
      subject: 'Corporate Payout Requested',
      text: 'Attached CSV contains all Payout pending records.',
      attachments: [{ filename: 'data.csv', path: filePath }],
    });

    console.log(`✅ Email sent to ${process.env.EMAIL_TO} with ${total} records.`);

    // Update processed documents
    if (idsToUpdate.length) {
      await collection.updateMany(
        { _id: { $in: idsToUpdate } },
        { $set: { cron_status: "success" } }
      );
      console.log(`✅ Updated ${idsToUpdate.length} records to cron_status = "success".`);
    }

  } catch (err) {
     if (fs.existsSync(filePath)) fs.unlinkSync(filePath);
    console.error('❌ Error:', err);
  } finally {
     if (fs.existsSync(filePath)) fs.unlinkSync(filePath);
    await client.close();
  }
}

fetchAndEmailData();


// fetchAndEmailData();

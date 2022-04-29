import { MongoClient } from "mongodb";
import fetch from "node-fetch";
import dotenv from "dotenv";
import sgMail from "@sendgrid/mail";

// fetch.default;
// import https from "https";

// const { MongoClient } = require("mongodb");
// const fetch = require("node-fetch");
// const dotenv = require("dotenv");
// const sgMail = require("@sendgrid/mail");

dotenv.config();

//MONGO CONNECTION
const uri = `mongodb+srv://${process.env.MONGO_USER}:${process.env.MONGO_PASS}@${process.env.MONGO_CLUSTER}/${process.env.MONGO_DB}?retryWrites=true&w=majority`;
const client = new MongoClient(uri);

//SENGRID
sgMail.setApiKey(process.env.SEND_GRID);

let msg = {
  to: process.env.EMAIL_TO, // Change to your recipient
  from: process.env.EMAIL_FROM, // Change to your verified sender
  subject: "test",
  text: "test",
  html: "test",
};

//duplicates and fuzzy match company name clean up
const company_dict = {
  "JP Morgan Chase": "JPMorgan Chase",
  JPMORGAN: "JPMorgan Chase",
  "JP Morgan": "JPMorgan Chase",
  JPMorgan: "JPMorgan Chase",
  "JP morgan": "JPMorgan Chase",
  "Jp Morgan": "JPMorgan Chase",
  "jp morgan": "JPMorgan Chase",
  "Jp morgan chase": "JPMorgan Chase",
  "Ford Motor": "Ford",
  "Ford Motor Company": "Ford",
  "Johnson and Johnson": "Johnson & Johnson",
  Juniper: "Juniper Networks",
  juniper: "Juniper Networks",
  HP: "HP Inc",
  "Hewlett Packard Enterprise": "HPE",
  Hsbc: "HSBC",
  "Amazon web services": "Amazon",
  "Apple Inc.": "Apple",
  "Bosch Global": "Bosch",
  "Deloitte Advisory": "Deloitte",
  "Deloitte Consulting": "Deloitte",
  "Deloitte consulting": "Deloitte",
  DISH: "DISH Network",
  "Dish Network": "DISH Network",
  Dish: "DISH Network",
  "Disney Streaming Services": "Disney",
  "The Walt Disney Company": "Disney",
  Epic: "Epic Systems",
  "Ernst and Young": "Ernst & Young",
  "Expedia Group": "Expedia",
  "Qualcomm Inc": "Qualcomm",
  "Raytheon Technologies": "Raytheon",
  MSFT: "Microsoft",
  "Microsoft Corporation": "Microsoft",
  Msft: "Microsoft",
  "microsoft corporation": "Microsoft",
  Snapchat: "Snap",
  "Sony Interactive Entertainment": "Sony",
  Micron: "Micron Technology",
  "Mckinsey & Company": "McKinsey",
  "Jane Street": "Jane Street Capital",
  EPAM: "EPAM Systems",
  "Costco Wholesale": "Costco",
  "Akamai Technology": "Akamai",
  "Akamai Technologies": "Akamai",
  "Visa inc": "Visa",
  "Wipro Limited": "Wipro",
  Zoominfo: "Zoom",
  "Zillow Group": "Zillow",
};

let updatedItem = [];
let status = { status: false, msg: "" };
// let cachedDb = null;

// async function connectToDatabase() {
//   if (cachedDb) {
//     return cachedDb;
//   }
//   // Connect to our MongoDB database hosted on MongoDB Atlas
//   const client = await MongoClient.connect(MONGODB_URI);
//   // Specify which database we want to use
//   const db = await client.db("pursuit");
//   cachedDb = db;
//   return db;
// }

export const handler = async (event, context, callback) => {
  // console.log("test---");

  try {
    console.log(process.env.MONGO_USER);
    const response = await fetch("https://www.levels.fyi/js/salaryData.json");
    // console.log(response);
    const data = await response.json();
    // console.log(data);
    if (!response.ok) {
      throw new Error(data.message || "Error pulling salary data from Levels");
    }

    data.map((item) => {
      //removed unwatned columns
      let { cityid, rowNumber, dmaid, ...removedColumns } = item;

      //converted to number
      removedColumns = {
        ...removedColumns,
        //fix the inconsistent in the data, some input is 100000 while other is 100, this will messed up the average, have convert all to 1000
        totalyearlycompensation: numFormatter(
          Number(removedColumns.totalyearlycompensation)
        ),
        yearsofexperience: Number(removedColumns.yearsofexperience),
        yearsatcompany: Number(removedColumns.yearsatcompany),
        basesalary: Number(removedColumns.basesalary),
        stockgrantvalue: Number(removedColumns.stockgrantvalue),
        bonus: Number(removedColumns.bonus),
      };

      if (removedColumns.company in company_dict) {
        removedColumns = {
          ...removedColumns,
          company: [company_dict[removedColumns.company]],
        };
      }
      updatedItem.push(removedColumns);
    });
    await addToDatabase().catch(console.dir);
  } catch (error) {
    console.log(error);
    status.status = false;
    status.msg = error;
    msg.subject = "PURSUIT - FETCHING DATA FAILED";
    msg.html = `<p>${error}</p>`;
    await sgMail
      .send(msg)
      .then(() => {
        console.log("Email sent");
      })
      .catch((error) => {
        console.error(error);
      });
  }
};

async function addToDatabase() {
  try {
    await client.connect();
    const db = client.db();
    console.log(db);
    const removedCollection = await db.collection("levels").drop();
    console.log(removedCollection);
    const status = await db.collection("levels").insertMany(updatedItem);
    // console.log(status);
    msg.subject = "PURSUIT - FETCH DATA SUCCESSFUL";
    msg.html = `<p>Success</p>`;

    await sgMail
      .send(msg)
      .then(() => {
        console.log("Email sent");
      })
      .catch((error) => {
        console.error(error);
      });
  } catch (e) {
    console.log(e);
    status.status = false;
    status.msg = e;
    msg.subject = "PURSUIT - UPDATING DB WITH LEVELS DATA FAILED";
    msg.html = `<p>${e}</p>`;
    await sgMail
      .send(msg)
      .then(() => {
        console.log("Email sent");
      })
      .catch((error) => {
        console.error(error);
      });
  } finally {
    console.log("Closing");
    await client.close();
    process.exit(0);
  }
}

function numFormatter(num) {
  if (num >= 10000 && num < 1000000) {
    return (num / 1000).toFixed(0); // convert to K for number from > 1000 < 1 million
  } else if (num >= 1000000) {
    return (num / 1000).toFixed(0); // convert to M for number from > 1 million
  } else if (num < 1000) {
    return num; // if value < 1000, nothing to do
  }
}
// handler();

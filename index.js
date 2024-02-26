const express = require("express");
const multer = require("multer");
const csvParser = require("csv-parser");
const moment = require("moment");
const mysql = require("mysql2");
const cors = require("cors");
const Busboy = require("busboy");
const status = require("express-status-monitor")
const fs = require("fs"); 
const app = express();
const port = 3001;
app.use(cors());
app.use(status())
const storage = multer.memoryStorage();
const upload = multer({ storage: storage });
const config = {
  api: {
    basePath: "/", 
  },
};
let insertCounter = 0;

const db = mysql.createConnection({
  host: "localhost",
  user: "root",
  password: "12345678",
  database: "coastallife",
  multipleStatements: true,
  port: 3307,
});

db.connect((err) => {
  if (err) {
    console.error("Database connection failed: " + err.stack);
    process.exit(1);
  }
  console.log("Connected to the database");
});


app.post("/upload", async (req, res) => {
  const busboy = new Busboy({ headers: req.headers });

  busboy.on("file", (fieldname, file, filename) => {
    const filePath = config.api.basePath + filename;

    file.pipe(fs.createWriteStream(filePath));

    file.on("end", async () => {
      try {
        await processCSV("yourcsvName", filePath);
        res.status(200).json({
          success: true,
          message: "CSV processing completed successfully",
        });
      } catch (error) {
        console.error("Error processing CSV:", error.message);
        res.status(500).send("Internal Server Error");
      }
    });
  });

  req.pipe(busboy);
});

async function processCSV(csvName, filePath) {
  return new Promise((resolve, reject) => {
    let numConcurrent = 0;
    let paused = false;
    const maxConcurrent = 1000;

    const stream = fs.createReadStream(filePath)
      .on("error", (err) => {
        console.log("Error processing CSV:", err);
        reject(err);
      })
      .pipe(csvParser())
      .on("data", async (row) => {
        async function checkResume() {
          --numConcurrent;
          if (paused && numConcurrent < maxConcurrent) {
            paused = false;
            stream.resume();
          }
        }
        ++numConcurrent;
        try {
          createcsv(csvName, row);
          row.mls_failedListingDate = moment(
            row.mls_failedListingDate,
            "MM/DD/YYYY"
          ).format("YYYY-MM-DD");
          row.mls_maxListPriceDate = moment(
            row.mls_maxListPriceDate,
            "MM/DD/YYYY"
          ).format("YYYY-MM-DD");
          row.mls_minListPriceDate = moment(
            row.mls_minListPriceDate,
            "MM/DD/YYYY"
          ).format("YYYY-MM-DD");
          row.mls_originalListingDate = moment(
            row.mls_originalListingDate,
            "MM/DD/YYYY"
          ).format("YYYY-MM-DD");
          row.mls_soldDate = moment(row.mls_soldDate, "MM/DD/YYYY").format(
            "YYYY-MM-DD"
          );
          row.entryDate = moment(row.entryDate, "MM/DD/YYYY").format(
            "YYYY-MM-DD"
          );
          await insertIntoDatabase([row], Object.keys(row));

          checkResume();
        } catch (error) {
          checkResume();
        }
        if (numConcurrent >= maxConcurrent) {
          stream.pause();
          paused = true;
        }
      })
      .on("end", () => {
        console.log("Finished processing CSV");
        resolve(filePath);
      });
  });
}
async function createcsv(name, data) {
  const { hostname, port, ip } = data;
  let protocol = 'https';
  if (port === 80) {
    protocol = 'http';
  }
  const url = protocol + '://' + hostname;
  
  try {
    return url;
  } catch (error) {
    throw error;
  }
}
app.get("/getData", async (req, res) => {
  try {
    const {
      address_city,
      address_state,
      mls_propertyType,
      mls_propertySubtype,
      address_zip,
      address_county,
      mls_soldDate,
      mls_status,
      startDate,
      endDate,
      page = 1,
      pageSize = 20,
    } = req.query;

    let query = "SELECT * FROM coastallife";
    let countQuery = "SELECT COUNT(*) as total FROM coastallife";
    let conditions = [];
    const offset = (page - 1) * pageSize;

    if (address_city || address_state || mls_propertyType || mls_propertySubtype || address_zip || address_county || mls_soldDate || mls_status || startDate || endDate) {
      query += " WHERE ";
      countQuery += " WHERE ";
    }

    if (address_city) {
      conditions.push(`address_city = ${mysql.escape(address_city)}`);
    }

    if (address_state) {
      conditions.push(`address_state = ${mysql.escape(address_state)}`);
    }

    if (mls_propertyType) {
      conditions.push(`mls_propertyType = ${mysql.escape(mls_propertyType)}`);
    }

    if (mls_propertySubtype) {
      conditions.push(`mls_propertySubtype = ${mysql.escape(mls_propertySubtype)}`);
    }

    if (address_zip) {
      conditions.push(`address_zip = ${mysql.escape(address_zip)}`);
    }

    if (address_county) {
      conditions.push(`address_county = ${mysql.escape(address_county)}`);
    }

    if (mls_soldDate) {
      const formattedDate = moment(mls_soldDate, 'MM/DD/YYYY').format('YYYY-MM-DD');
      conditions.push(`mls_soldDate = ${mysql.escape(formattedDate)}`);
    }

    if (mls_status) {
      conditions.push(`mls_status = ${mysql.escape(mls_status)}`);
    }

    if (startDate && endDate) {
      const formattedStartDate = moment(startDate, 'MM/DD/YYYY').format('YYYY-MM-DD');
      const formattedEndDate = moment(endDate, 'MM/DD/YYYY').format('YYYY-MM-DD');
      conditions.push(`entryDate BETWEEN ${mysql.escape(formattedStartDate)} AND ${mysql.escape(formattedEndDate)}`);
    } else if (startDate) {
      const formattedStartDate = moment(startDate, 'MM/DD/YYYY').format('YYYY-MM-DD');
      conditions.push(`entryDate = ${mysql.escape(formattedStartDate)}`);
    }

    if (conditions.length > 0) {
  query += conditions.join(" AND ");
  countQuery += conditions.join(" AND ");
}

    query += ` ORDER BY entryDate LIMIT ${pageSize} OFFSET ${offset}`;

    const [result, totalItems] = await Promise.all([
      db.promise().query(query),
      db.promise().query(countQuery),
    ]);
    console.log("Generated query:", query);

    res.status(200).json({ data: result[0], totalItems: totalItems[0][0].total });
  } catch (error) {
    console.error("Error handling data retrieval:", error.message);
    res.status(500).send("Internal Server Error");
  }
});
console.log("")
async function insertIntoDatabase(data, columns) {
  try {
    if (!columns || columns.length === 0) {
      throw new Error("Columns are undefined or empty");
    }

    const batchSize = 1300; 
    const totalRows = data.length;

    for (let i = 0; i < totalRows; i += batchSize) {
      const batchData = data.slice(i, i + batchSize);
      const batchValues = [];

      for (const record of batchData) {
        batchValues.push(...columns.map((column) => record[column]));
      }

      const columnNames = columns
  .map((column) => mysql.escapeId(column))
  .join(", ");
const placeholders = Array(columns.length)
  .fill("?")
  .join(", ");
const placeholdersPerRow = Array(batchData.length)
  .fill(`(${placeholders})`)
  .join(", ");

      const sql = `INSERT IGNORE INTO coastallife (${columnNames}) VALUES ${placeholdersPerRow}`;

      await db.promise().query(sql, batchValues);
      insertCounter++;

    }

    console.log(`Data inserted into the database successfully, ${insertCounter} rows inserted`);
    return {
      success: true,
      message: "Data inserted into the database successfully",
    };
  } catch (error) {
    console.error("Error handling data insertion:", error.message);

    const response = {
      success: false,
      message: "Error inserting data into the database",
    };

    if (error && error.code === "ER_BAD_FIELD_ERROR") {
      response.errorType = "unknown_column";
      response.message = "Unknown column in the database";
    }

    return response;
  }
}



app.listen(port, () => {
  console.log(`Server is running on port ${port}`);
});

const express = require("express");
const multer = require("multer");
const csvParser = require("csv-parser");
const moment = require("moment");
const mysql = require("mysql2");
const cors = require("cors");

const app = express();
const port = 3001;
app.use(cors());

const storage = multer.memoryStorage();
const upload = multer({ storage: storage });

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

app.post("/upload", upload.single("file"), async (req, res) => {
  try {
    const fileBuffer = req.file.buffer;

    const results = [];
    let columns;

  const readableStream = require("stream").Readable;
      const stream = new readableStream();
      stream.push(fileBuffer);
      stream.push(null);
  
      stream
        .pipe(csvParser({ delimiter: "," }))
        .on("data", (data) => {
          if (!columns) {
            columns = Object.keys(data);
          }
          data.mls_failedListingDate = moment(
            data.mls_failedListingDate,
            "DD/MM/YYYY"
          ).format("YYYY-MM-DD");
          data.mls_maxListPriceDate = moment(
            data.mls_maxListPriceDate,
            "DD/MM/YYYY"
          ).format("YYYY-MM-DD");
          data.mls_minListPriceDate = moment(
            data.mls_minListPriceDate,
            "DD/MM/YYYY"
          ).format("YYYY-MM-DD");
          data.mls_originalListingDate = moment(
            data.mls_originalListingDate,
            "DD/MM/YYYY"
          ).format("YYYY-MM-DD");
          data.mls_soldDate = moment(data.mls_soldDate, "DD/MM/YYYY").format(
            "YYYY-MM-DD"
          );
          data.entryDate = moment(data.entryDate, "DD/MM/YYYY").format(
            "YYYY-MM-DD"
          );
          results.push(data);
        })
        .on("end", () => {
          insertIntoDatabase(results, columns, res);
        });
    } catch (error) {
      console.error("Error handling file upload:", error.message);
      res.status(500).send("Internal Server Error");
    }
  });

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
      endDate
    } = req.query;

    let query = "SELECT * FROM coastallife";
    let countQuery = "SELECT COUNT(*) as total FROM coastallife";
    let conditions = [];

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

    query += " ORDER BY entryDate";

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
async function insertIntoDatabase(data, columns, res) {
  try {
    if (!columns || columns.length === 0) {
      throw new Error("Columns are undefined or empty");
    }

    const columnNames = columns
      .map((column) => mysql.escapeId(column))
      .join(", ");
    const placeholders = Array(columns.length)
      .fill("?")
      .join(", ");
    const values = [];

    for (const record of data) {
      values.push(...columns.map((column) => record[column]));
    }

    const rowCount = data.length;
    const placeholdersPerRow = Array(rowCount)
      .fill(`(${placeholders})`)
      .join(", ");

    const sql = `INSERT IGNORE INTO coastallife (${columnNames}) VALUES ${placeholdersPerRow}`;

    await db.promise().query(sql, values);

    console.log("Data inserted into the database successfully");
    res.status(200).json({
      success: true,
      message: "Data inserted into the database successfully",
    });
  } catch (error) {
    console.error("Error handling data insertion:", error.message);
    if (error.code === "ER_BAD_FIELD_ERROR") {
      res.status(500).json({
        success: false,
        message: "Unknown column in database",
        errorType: "unknown_column",
      });
    } else {
      res
        .status(500)
        .json({
          success: false,
          message: "Error inserting data into the database",
        });
    }
  }
}

app.listen(port, () => {
  console.log(`Server is running on port ${port}`);
});

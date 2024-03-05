"use strict";

import { Queue, Worker } from "bullmq";
import Busboy from "busboy";
import cors from "cors";
import { createHash } from "crypto";
import csvParser from "csv-parser";
import express from "express";
import status from "express-status-monitor";
import { createReadStream, createWriteStream } from "fs";
import http from "http";
import { parse } from "json2csv";
import { DateTime } from "luxon";
import { createConnection, escape, escapeId } from "mysql2/promise";

const { fromFormat } = DateTime;
const app = express();
app.use(
  cors({
    origin: "http://localhost:3000",
  })
);
app.use(status());

const config = {
  api: {
    basePath: "/",
  },
};
const redisConnection = {
  host: "localhost",
  port: 6379,
};
const queue = new Queue("CoastalLife", { connection: redisConnection });
let insertCounter = 0;

new Worker(
  "CoastalLife",
  async (job) => {
    if (job.name === "process-document") {
      await processCSV(job.data.filePath, job.data.totalRows, job.id);
    }
  },
  {
    concurrency: 100,
    connection: redisConnection,
  }
);

const db = await createConnection({
  host: "localhost",
  user: "root",
  password: "my-secret-pw",
  database: "CoastalLife",
  multipleStatements: true,
});

app.post("/upload", (req, res) => {
  const busboy = Busboy({ headers: req.headers });

  busboy.on("file", (fieldname, file, { filename }) => {
    const filePath = config.api.basePath + filename;
    let totalRows = 0;

    file.pipe(createWriteStream(filePath));
    file.on("data", function (chunk) {
      for (let i = 0; i < chunk.length; ++i) {
        if (chunk[i] == 10) totalRows++;
      }
    });
    file.on("end", async () => {
      try {
        const job = await queue.add("process-document", {
          filePath,
          totalRows,
          id: req.query.id,
          startingTime: new Date(),
        });
        res.status(200).json({
          success: true,
          message: "CSV processing completed successfully",
          totalRows,
          jobId: job.id,
        });
      } catch (error) {
        console.error("Error processing CSV:", error.message);
        res.status(500).send("Internal Server Error");
      }
    });
  });

  req.pipe(busboy);
});

const calculateUniqueHash = (data) => {
  return createHash("sha256")
    .update(
      data.mls_status +
      data.address_state +
      data.address_zip +
      data.address_county +
      data.address_city +
      data.address_houseNumber +
      data.address_street
    )
    .digest("base64");
};

async function processCSV(filePath, totalRows, jobId) {
  return new Promise((resolve, reject) => {
    let numConcurrent = 0;
    const maxConcurrent = 1000;
    let rows = [];
    let rowsProcessed = 0;

    const stream = createReadStream(filePath)
      .on("error", (err) => {
        console.log("Error processing CSV:", err);
        reject(err);
      })
      .pipe(csvParser())
      .on("data", async (row) => {
        numConcurrent++;
        rowsProcessed++;
        if (row.entryDate) {
          row.entryDate = fromFormat(row.entryDate, "MM/DD/YYYY").toFormat(
            "YYYY-MM-DD"
          );
        }
        if (row.mls_failedListingDate) {
          row.mls_failedListingDate = fromFormat(
            row.mls_failedListingDate,
            "MM/DD/YYYY"
          ).toFormat("YYYY-MM-DD");
        }
        if (row.mls_maxListPriceDate) {
          row.mls_maxListPriceDate = fromFormat(
            row.mls_maxListPriceDate,
            "MM/DD/YYYY"
          ).toFormat("YYYY-MM-DD");
        }
        if (row.mls_minListPriceDate) {
          row.mls_minListPriceDate = fromFormat(
            row.mls_minListPriceDate,
            "MM/DD/YYYY"
          ).toFormat("YYYY-MM-DD");
        }
        if (row.mls_originalListingDate) {
          row.mls_originalListingDate = fromFormat(
            row.mls_originalListingDate,
            "MM/DD/YYYY"
          ).toFormat("YYYY-MM-DD");
        }
        if (row.mls_soldDate) {
          row.mls_soldDate = fromFormat(
            row.mls_soldDate,
            "MM/DD/YYYY"
          ).toFormat("YYYY-MM-DD");
        }
        row.uniqueHash = calculateUniqueHash(row);
        rows.push(row);

        if (numConcurrent >= maxConcurrent) {
          stream.pause();

          await Promise.all([
            insertIntoDatabase(rows, Object.keys(row)),
            queue.updateJobProgress(jobId, {
              progress: (rowsProcessed / totalRows) * 100,
              rowsProcessed,
              totalRows,
            }),
          ]);
          stream.resume();
          rows = [];
          numConcurrent = 0;
        }
      })
      .on("end", async () => {
        console.log("Finished processing CSV");
        const job = await queue.getJob(jobId);
        job.updateData({
          ...job.data,
          endingDate: new Date(),
        });
        if (rows.length > 0) {
          await Promise.all([
            insertIntoDatabase(rows, Object.keys(rows[0])),
            job.updateProgress({
              progress: 100,
              rowsProcessed,
              totalRows,
            }),
          ]);

          rows = [];
          numConcurrent = 0;
        }
        resolve();
      });
  });
}

app.get("/getData", async (req, res) => {
  try {
    const { query, countQuery } = getSqlQuery(req);

    const [result, totalItems] = await Promise.all([
      db.query(query),
      db.query(countQuery),
    ]);
    console.log("Generated query:", query);
    console.log("Generated query:", countQuery);

    res
      .status(200)
      .json({ data: result[0], totalItems: totalItems[0][0].total });
  } catch (error) {
    console.error("Error handling data retrieval:", error.message);
    res.status(500).send("Internal Server Error");
  }
});

app.get("/progress", async (req, res) => {
  const { jobId } = req.query;
  const job = await queue.getJob(jobId);

  res.status(200).json({ ...job.progress, ...job.data });
});

async function insertIntoDatabase(data, columns) {
  try {
    if (!columns || columns.length === 0) {
      throw new Error("Columns are undefined or empty");
    }

    const insertValues = [];
    for (const record of data) {
      insertValues.push(...columns.map((column) => record[column]));
    }

    const columnNames = columns.map((column) => escapeId(column)).join(", ");
    const placeholders = Array(columns.length).fill("?").join(", ");
    const placeholdersPerRow = Array(data.length)
      .fill(`(${placeholders})`)
      .join(", ");

    const sql = `INSERT IGNORE INTO Document (${columnNames}) VALUES ${placeholdersPerRow}`;

    await db.query(sql, insertValues);
    insertCounter++;

    console.log(
      `Data inserted into the database successfully, ${insertValues.length} rows inserted`
    );
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

const server = http.createServer(app);

server.listen(3001, () => {
  console.log("listening on *:3001");
});

app.get("/exportCSV", async (req, res) => {
  try {
    const { query } = getSqlQuery(req, true);
    const [rows] = await db.query(query);

    if (rows.length === 0) {
      return res.status(404).send("No data found to export.");
    }

    const csvFields = Object.keys(rows[0]);
    const csvData = parse(rows, { fields: csvFields });

    res.setHeader("Content-Type", "text/csv");
    res.setHeader("Content-Disposition", 'attachment; filename="data.csv"');
    res.status(200).send(csvData);
  } catch (error) {
    console.error("Error exporting data as CSV:", error.message);
    res.status(500).send("Internal Server Error");
  }
});

function getSqlQuery(req, isExport) {
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

  let query = "SELECT * FROM Document";
  let countQuery = "SELECT COUNT(*) as total FROM Document";
  let conditions = [];
  const offset = (page - 1) * pageSize;

  if (
    address_city ||
    address_state ||
    mls_propertyType ||
    mls_propertySubtype ||
    address_zip ||
    address_county ||
    mls_soldDate ||
    mls_status ||
    startDate ||
    endDate
  ) {
    query += " WHERE ";
    countQuery += " WHERE ";
  }

  if (address_city) {
    conditions.push(`address_city = ${escape(address_city)}`);
  }

  if (address_state) {
    conditions.push(`address_state = ${escape(address_state)}`);
  }

  if (mls_propertyType) {
    conditions.push(`mls_propertyType = ${escape(mls_propertyType)}`);
  }

  if (mls_propertySubtype) {
    conditions.push(`mls_propertySubtype = ${escape(mls_propertySubtype)}`);
  }

  if (address_zip) {
    conditions.push(`address_zip = ${escape(address_zip)}`);
  }

  if (address_county) {
    conditions.push(`address_county = ${escape(address_county)}`);
  }

  if (mls_soldDate) {
    const formattedDate = fromFormat(mls_soldDate, "MM/DD/YYYY").toFormat(
      "YYYY-MM-DD"
    );
    conditions.push(`mls_soldDate = ${escape(formattedDate)}`);
  }

  if (mls_status) {
    conditions.push(`mls_status = ${escape(mls_status)}`);
  }

  if (startDate && endDate) {
    const formattedStartDate = fromFormat(startDate, "MM/DD/YYYY").toFormat(
      "YYYY-MM-DD"
    );
    const formattedEndDate = fromFormat(endDate, "MM/DD/YYYY").toFormat(
      "YYYY-MM-DD"
    );
    conditions.push(
      `entryDate BETWEEN ${escape(formattedStartDate)} AND ${escape(
        formattedEndDate
      )}`
    );
  } else if (startDate) {
    const formattedStartDate = fromFormat(startDate, "MM/DD/YYYY").toFormat(
      "YYYY-MM-DD"
    );
    conditions.push(`entryDate = ${escape(formattedStartDate)}`);
  }

  if (conditions.length > 0) {
    query += conditions.join(" AND ");
    countQuery += conditions.join(" AND ");
  }
  if (isExport) {
    query += ` ORDER BY entryDate`;
  }
  else {
    query += ` ORDER BY entryDate LIMIT ${pageSize} OFFSET ${offset}`;
  }
  return { query, countQuery };
}

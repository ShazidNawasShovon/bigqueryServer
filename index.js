const express = require("express");
const app = express();
const cors = require("cors");


const { BigQuery } = require('@google-cloud/bigquery');
const port = process.env.PORT || 3000;

app.use(cors());
app.use(express.json());
const bigQuery = new BigQuery({
  projectId: 'toffee-261507',
  keyFilename: 'bigquery-service-account.json',
});
app.post('/insertData', async (req, res) => {
  const data = req.body.viewerData;
  try {
    const datasetId = 'iptv';
    const tableId = 'current_viewers_heartbeat';
    // Check if dataset exists
    const [datasets] = await bigQuery.getDatasets();
    const dataset = datasets.find(ds => ds.id === datasetId);

    if (!dataset) {
      // Create the dataset if it doesn't exist
      await bigQuery.createDataset(datasetId);
      console.log(`Dataset "${datasetId}" created.`);
    } else {
      console.log(`Dataset "${datasetId}" already exists.`);
    }

    // Get or create the table
    const datasetRef = bigQuery.dataset(datasetId);
    const [tables] = await datasetRef.getTables();
    const table = tables.find(tbl => tbl.id === tableId);

    if (!table) {
      // Create the table if it doesn't exist
      await datasetRef.createTable(tableId);
      console.log(`Table "${tableId}" created in dataset "${datasetId}".`);
    } else {
      console.log(`Table "${tableId}" already exists in dataset "${datasetId}".`);

      // Insert data into the table
      console.log('Data to Insert:', data);
      const [apiResponse] = await table.insert(data);
      console.log('Inserted:', apiResponse);
      if (apiResponse && apiResponse.insertErrors) {
        console.error('Insertion Errors:', apiResponse.insertErrors);
        // Handle errors here
      }
    }
    console.log(res)
    res.json({ success: true });

  } catch (error) {
    console.error('Error:', error);
    res.status(500).json({ success: false, error: 'Internal Server Error' });
  }
});
app.get('/getData', async (req, res) => {
  try {
    const datasetId = 'iptv';
    const tableId = 'current_viewers_heartbeat';

    const datasetRef = bigQuery.dataset(datasetId);
    const table = datasetRef.table(tableId);

    const [rows] = await table.getRows();

    res.json({ success: true, data: rows });
  } catch (error) {
    console.error('Error:', error);
    res.status(500).json({ success: false, error: 'Internal Server Error' });
  }
});


app.get("/", (req, res) => {
  res.send("Bigquery Server is Ready to send data to the server....!");
});

app.listen(port, () => {
  console.log(`Server running at http://localhost:${port}`);
});

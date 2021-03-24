const express = require('express')
const app = express()
const fs = require('fs');
const { Parser } = require('json2csv');
const { parse } = require('json2csv');
const cors = require('cors')
var mysql = require('mysql');
const fastcsv = require('fast-csv')
var bodyParser = require('body-parser');
var con = mysql.createConnection({
  host: "localhost",
  user: "root",
  database:"nithin",
  password: "nithin@123/"
});

con.connect(function(err) {
  if (err) throw err;
  console.log("Connected!");
});


app.use(cors())

app.use(bodyParser.json({limit: '50mb'}));
app.use(bodyParser.urlencoded({limit: '50mb', extended: true}));
app.get('/',(req,res)=>{
    res.send("hi mama");
})
app.post('/data/hh',(req,res)=>{
   const x = req.body.data;
   const fields = ["HSHD_NUM","L","AGE_RANGE","MARITAL","INCOME_RANGE","HOMEOWNER","HSHD_COMPOSITION","HH_SIZE","CHILDREN"]
   const opts = {fields}
    try {
      const csv = parse(x, opts);
      fs.writeFileSync('./hoh.csv', csv);
    } catch (err) {
      console.error(err);
    }
    let stream = fs.createReadStream('./hoh.csv');
let csvData = [];
var refer = 0;
let csvStream = fastcsv
  .parse()
  .on("data", function(data) {
    if(refer == 0)
    {
      refer = 1;
    }
    else{
      csvData.push(data);
    }
  })
  .on("end", function() {
    // remove the first line: header
    csvData.shift();

    // create a new connection to the database
    const connection = mysql.createConnection({
      host: "localhost",
      user: "root",
      password: "nithin@123/",
      database: "nithin"
    });

    // open the connection
    connection.connect(error => {
      if (error) {
        console.error(error);
      } else {
        let query =
          "INSERT INTO HH VALUES ?";
        connection.query(query, [csvData], (error, response) => {
          console.log(error || response);
        });
      }
    });
  });

stream.pipe(csvStream);
    
})
//===========================================================================
app.post('/data/pr',(req,res)=>{
  const x = req.body.data;
  const fields = ["PRODUCT_NUM","DEPARTMENT","COMMODITY","BRAND_TY","NATURAL_ORGANIC_FLAG"]
  const opts = {fields}
   try {
     const parser = new Parser(opts);
     const csv = parser.parse(x);
     fs.writeFileSync('./pr.csv', csv);
   } catch (err) {
     console.error(err);
   }
   let stream = fs.createReadStream('./pr.csv');
let csvData = [];
var refer = 0;
let csvStream = fastcsv
 .parse()
 .on("data", function(data) {
   if(refer == 0)
   {
     refer = 1;
   }
   else{
    csvData.push(data);
   }
   
 })
 .on("end", function() {
   // remove the first line: header
   csvData.shift();

   // create a new connection to the database
   const connection = mysql.createConnection({
     host: "localhost",
     user: "root",
     password: "nithin@123/",
     database: "nithin"
   });

   // open the connection
   connection.connect(error => {
     if (error) {
       console.error(error);
     } else {
       let query =
         "INSERT INTO PR VALUES ?";
       connection.query(query, [csvData], (error, response) => {
         console.log(error || response);
       });
     }
   });
 });

stream.pipe(csvStream);
   
})
//=======================================================
app.post('/data/tr',(req,res)=>{
  const x = req.body.data;
  const fields = ["BASKET_NUM","HSHD_NUM","PURCHASE_","PRODUCT_NUM","SPEND","UNITS","STORE_R","WEEK_NUM","YEAR"]
  const opts = {fields}
   try {
     const parser = new Parser(opts);
     const csv = parser.parse(x);
     fs.writeFileSync('./tr.csv', csv);
   } catch (err) {
     console.error(err);
   }
   let stream = fs.createReadStream('./tr.csv');
let csvData = [];
var refer = 0;
let csvStream = fastcsv
 .parse()
 .on("data", function(data) {
   if(refer == 0)
   {
     refer = 1;
   }
   else
   {
    csvData.push(data);
   }
 })
 .on("end", function() {
   // remove the first line: header
   csvData.shift();

   // create a new connection to the database
   const connection = mysql.createConnection({
     host: "localhost",
     user: "root",
     password: "nithin@123/",
     database: "nithin"
   });

   // open the connection
   connection.connect(error => {
     if (error) {
       console.error(error);
     } else {
       let query =
         "INSERT INTO TR VALUES ?";
       connection.query(query, [csvData], (error, response) => {
         console.log(error || response);
       });
     }
   });
 });

stream.pipe(csvStream);
   
})
app.listen(5000,()=>{
    console.log("connected to port 5000")
})
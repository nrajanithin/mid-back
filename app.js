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
  const sql = `select sum(spend) as ss,store_r from tr, pr where pr.PRODUCT_NUM=tr.PRODUCT_NUM and pr.DEPARTMENT='PHARMA' group
  by store_r;`
  con.query(sql, function (err, result) {
    if (err) 
    {
      throw err;
    }
    console.log("Result: " + result);
    res.send(result);
  });
  
})
app.get('/food',(req,res)=>{
  const sql = `select sum(spend) as ss,store_r from tr, pr where pr.PRODUCT_NUM=tr.PRODUCT_NUM and pr.DEPARTMENT='FOOD' group
  by store_r;`
  con.query(sql, function (err, result) {
    if (err) 
    {
      throw err;
    }
    console.log("Result: " + result);
    res.send(result);
  });
  
})
app.get('/nonfood',(req,res)=>{
  const sql = `select sum(spend) as ss,store_r from tr, pr where pr.PRODUCT_NUM=tr.PRODUCT_NUM and pr.DEPARTMENT='NON-FOOD' group
  by store_r;`
  con.query(sql, function (err, result) {
    if (err) 
    {
      throw err;
    }
    console.log("Result: " + result);
    res.send(result);
  });
  
})
app.get('/line',(req,res)=>{
  const sql = `select sum(SPEND) as ss,store_r, 'JAN' as Month from tr where PURCHASE_ like "%JAN%" group by store_r union 
  select sum(SPEND) as ss,store_r, 'FEB' as Month from tr where PURCHASE_ like "%FEB%" group by store_r union 
  select sum(SPEND) as ss,store_r, 'MAR' as Month from tr where PURCHASE_ like "%MAR%" group by store_r union 
  select sum(SPEND) as ss,store_r, 'APR' as Month from tr where PURCHASE_ like "%APR%" group by store_r union
  select sum(SPEND) as ss,store_r, 'MAY' as Month from tr where PURCHASE_ like "%MAY%" group by store_r union 
  select sum(SPEND) as ss,store_r, 'JUN' as Month from tr where PURCHASE_ like "%JUN%" group by store_r union 
  select sum(SPEND) as ss,store_r, 'JUL' as Month from tr where PURCHASE_ like "%JUL%" group by store_r union 
  select sum(SPEND) as ss,store_r, 'AUG' as Month from tr where PURCHASE_ like "%AUG%" group by store_r union 
  select sum(SPEND) as ss,store_r, 'SEP' as Month from tr where PURCHASE_ like "%SEP%" group by store_r union 
  select sum(SPEND) as ss,store_r, 'OCT' as Month from tr where PURCHASE_ like "%OCT%" group by store_r union 
  select sum(SPEND) as ss,store_r, 'NOV' as Month from tr where PURCHASE_ like "%NOV%" group by store_r union 
  select sum(SPEND) as ss,store_r, 'DEC' as Month from tr where PURCHASE_ like "%DEC%" group by store_r`
  con.query(sql, function (err, result) {
    if (err) 
    {
      throw err;
    }
    console.log("Result: " + result);
    res.send(result);
  });
})
app.get('/don',(req,res)=>{
  const sql = `select sum(spend) as ss,INCOME_RANGE from hh,tr where tr.HSHD_NUM = hh.HSHD_NUM group by INCOME_RANGE;`
  con.query(sql, function (err, result) {
    if (err) 
    {
      throw err;
    }
    console.log("Result: " + result);
    res.send(result);
  });
})
app.get('/bar',(req,res)=>{
  const sql =`select sum(spend) as ss, department, "JAN" as MONTH from tr, pr where tr.PRODUCT_NUM=pr.PRODUCT_NUM and PURCHASE_ like '%JAN%' GROUP BY department union
   select sum(spend) as ss, department, "FEB" as MONTH from tr, pr where tr.PRODUCT_NUM=pr.PRODUCT_NUM and PURCHASE_ like '%FEB%' GROUP BY department union 
   select sum(spend) as ss, department, "MAR" as MONTH from tr, pr where tr.PRODUCT_NUM=pr.PRODUCT_NUM and PURCHASE_ like '%MAR%' GROUP BY department union 
   select sum(spend) as ss, department, "APR" as MONTH from tr, pr where tr.PRODUCT_NUM=pr.PRODUCT_NUM and PURCHASE_ like '%APR%' GROUP BY department union 
   select sum(spend) as ss, department, "MAY" as MONTH from tr, pr where tr.PRODUCT_NUM=pr.PRODUCT_NUM and PURCHASE_ like '%MAY%' GROUP BY department union 
   select sum(spend) as ss, department, "JUN" as MONTH from tr, pr where tr.PRODUCT_NUM=pr.PRODUCT_NUM and PURCHASE_ like '%JUN%' GROUP BY department union 
   select sum(spend) as ss, department, "JUL" as MONTH from tr, pr where tr.PRODUCT_NUM=pr.PRODUCT_NUM and PURCHASE_ like '%JUL%' GROUP BY department union 
   select sum(spend) as ss, department, "AUG" as MONTH from tr, pr where tr.PRODUCT_NUM=pr.PRODUCT_NUM and PURCHASE_ like '%AUG%' GROUP BY department union 
   select sum(spend) as ss, department, "SEP" as MONTH from tr, pr where tr.PRODUCT_NUM=pr.PRODUCT_NUM and PURCHASE_ like '%SEP%' GROUP BY department union 
   select sum(spend) as ss, department, "OCT" as MONTH from tr, pr where tr.PRODUCT_NUM=pr.PRODUCT_NUM and PURCHASE_ like '%OCT%' GROUP BY department union 
   select sum(spend) as ss, department, "NOV" as MONTH from tr, pr where tr.PRODUCT_NUM=pr.PRODUCT_NUM and PURCHASE_ like '%NOV%' GROUP BY department union 
   select sum(spend) as ss, department, "DEC" as MONTH from tr, pr where tr.PRODUCT_NUM=pr.PRODUCT_NUM and PURCHASE_ like '%DEC%' GROUP BY department`
  con.query(sql, function (err, result) {
    if (err) 
    {
      throw err;
    }
    console.log("Result: " + result);
    res.send(result);
  });
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
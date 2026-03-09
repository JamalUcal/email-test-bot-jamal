const { userInputData } = require("./config")
const fs = require('fs');
const csv = require('csvtojson');
const convert = require('json-2-csv');
var prompts = require('prompts');
var csvFilePath = "";

const brand_partnumberlengthcsv = "../Brand_partNumber.csv"
var input_partNumber = userInputData.partNumber;
var input_description = userInputData.description;
var input_formerPartNumber = userInputData.formerPartNumber;
var input_supersedePartNumber = userInputData.supersedePartNumber;
var input_price = userInputData.price
let partNumberLength;

const questions = [
    {
        type: 'text',
        name: 'brandName',
        message: 'Please enter Brand Name:'
    },
    {
        type: 'text',
        name: 'supplierName',
        message: 'Please enter Supplier Name:'
    },
    {
        type: 'text',
        name: 'Location',
        message: 'Please enter location:'
    },
    {
        type: 'text',
        name: 'currency',
        message: 'Please enter Currency:'
    },
    {
        type: 'number',
        name: 'spliceBrandNamefromPartNumber',
        message: 'Please enter No of prefixes to be removed from Part Number:'
    }
];

async function myFunction() {
    var response = await prompts(questions);
    var { brandName, supplierName, Location, currency, spliceBrandNamefromPartNumber } = response;
    const brand_partnumberlength = await csv().fromFile(brand_partnumberlengthcsv);
    partNumberLength = brand_partnumberlength.find(el => el.Brand === brandName.toUpperCase());
    if (!partNumberLength) {
        console.log('%c ' + "Error:Please check the Brand Name", 'background: red; color: white');
        // console.error("Error:Please check the Brand Name".red);
        return;
    }
    console.log("Standard Part Number Length for the Brand is :" + partNumberLength["Part_Length"]);
    fs.readdirSync("../../Supplier_pricelist/").forEach(file => {
        csvFilePath = "../../Supplier_pricelist/" + file;
    });
    const jsonArray = await csv().fromFile(csvFilePath);
    var finalarr = [];
    jsonArray.forEach((data, index) => {
        var obj = {};
        obj["Brand"] = brandName.toUpperCase();
        obj["Supplier Name"] = supplierName;
        obj["Location"] = Location.toUpperCase();
        obj["Currency"] = currency.toUpperCase();
        obj["Part Number"] = processPartnumber(spliceBrandNamefromPartNumber, data[input_partNumber], index);
        obj["Description"] = data[input_description];
        obj["FORMER PN"] = processPartnumber(spliceBrandNamefromPartNumber,data[input_formerPartNumber]);
        obj["SUPERSESSION"] = processPartnumber(spliceBrandNamefromPartNumber,data[input_supersedePartNumber]);
        if (data[input_price]) {
            obj["Price"] = data[input_price].replace(/[^0-9.]/gi, '');
            obj["Price"] = parseFloat(obj["Price"]).toFixed(2)
        } else {
            console.log("Warning: Price is empty in the " + (index + 2));
            obj["Price"] = "";
        }
        finalarr.push(obj)
    })
    var m_names = ['JAN', 'FEB', 'MAR',
        'APR', 'MAY', 'JUN', 'JUL',
        'AUG', 'SEP', 'OCT', 'NOV', 'DEC'];
    d = new Date();
    var month = m_names[d.getMonth()];
    var year = d.getFullYear();
    var outputFileName = brandName + "_" + supplierName + "_" + currency + "_" + Location + "_" + month + "_" + year;
    const output_csv = await convert.json2csv(finalarr);
    fs.writeFileSync(outputFileName + '.csv', output_csv)
}
function processPartnumber(spliceBrandNamefromPartNumber, partnumber, indexdata = null) {
    if (partnumber) {
        partnumber = partnumber.replace(/[^a-z0-9]/gi, '');
        partnumber = partnumber.toString().substring(spliceBrandNamefromPartNumber);
        return partnumber.padStart(partNumberLength.Part_Length, 0);
    }
    else {
        if (indexdata) {
            console.log("Warning:Part Number is empty in the row " + (indexdata + 2));
        }
        return "";
    }
}
myFunction()
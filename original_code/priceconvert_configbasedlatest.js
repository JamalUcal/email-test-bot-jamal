const fs = require('fs-extra');
var prompts = require('prompts');
const csv = require('csvtojson');
const convert = require('json-2-csv');
var csvFilePath = "";
var minimumPartNumberLength;
var supplier = [];
var brand = [];
var location;
var currency;
var inputBrand;
var discount;
var gst;
var supplierElement;
var brandElement;
var columnRead = [];
var isgstempty = false;
var priceEmptyCount = 0;
var partNumberEmptyCount = 0;
var gstNaArray = [];
var priceNaArray = [];
var priceAlphaArray = [];
var partNumberNAarray = [];
var gstNAN = false;
var gstNaNArray = [];
const { createLogger, format, transports } = require('winston');
const { combine, timestamp, label, printf } = format;
const myFormat = printf(({ level, message, label, timestamp }) => {
    return `${timestamp} ${level}: ${message}`;
});
const logger = createLogger({
    format: combine(
        label({ label: '' }),
        timestamp(),
        myFormat
    ),
    transports: [
        new transports.File({ filename: 'combined.log' }),
    ],
});
logger.info('Priceconvert has intiated');
//Count the csv files in the Supplier_pricelist folder
const dirCont = fs.readdirSync("./Supplier_pricelist/");
const csvCount = dirCont.filter((elm) => elm.match(/.*\.(csv?)/ig)).length;
logger.info("No of csv files in the input folder: " + csvCount);
console.log("No of csv files in the input folder: " + csvCount);
//To show the error to user,if input folder has multiple csv files
if (csvCount > 1) {
    logger.error("Input folder had " + csvCount + " files");
    console.log('\x1b[31m%s\x1b[0m', "Input folder has multiple files")
    return false;
}
//Read the input price file
fs.readdirSync("./Supplier_pricelist/").forEach(file => {
    if (file.match(/.*\.(csv?)/ig)) {
        csvFilePath = "./Supplier_pricelist/" + file;
    }
});
// To show the error to user, if input folder has no file
if (!csvFilePath) {
    logger.error("Input folder has no csv files");
    console.log('\x1b[31m%s\x1b[0m', "Please add the input file!");
    return false;
}
// Show the list of Suppliers to user, prompts the user to enter the supplier name
async function promptSupplier(question) {
    console.log("List of Suppliers")
    console.log(supplier.sort())
    var response = await prompts(question);
    var { supplierName } = response;
    var inputSupplier1 = supplierName.toUpperCase();
    if (supplier.indexOf(inputSupplier1) == -1) {
        logger.error("Supplier Name prompted by the user is " + inputSupplier1 + " and is not found in the supplier configuration file");
        console.log('\x1b[31m%s\x1b[0m', "Please enter the valid supplier Name from the list");
        return await promptSupplier(question);
    }
    else {
        logger.info("Supplier Name entered by the User :" + inputSupplier1);
        console.log('\x1b[32m%s\x1b[0m', "Supplier Name is valid")
        return response;
    }
}
// Show the list of brand to user, prompts the user to enter the brand name
async function promptBrand(question) {
    console.log("Please find the list of Brands");
    console.log(brand.sort());
    var response = await prompts(question);
    var { brandName } = response;
    inputBrand = brandName.toUpperCase();
    if (brand.indexOf(inputBrand) == -1) {
        logger.error("Brand Name prompted by the user is " + inputBrand + " and is not found in the supplier configuration file");
        console.log('\x1b[31m%s\x1b[0m', "Please enter the valid Brand Name from the list");
        return await promptBrand(question);
    }
    else {
        logger.info("Brand Name entered by the User :" + inputBrand);
        console.log('\x1b[32m%s\x1b[0m', "Brand Name is valid")
        return response;
    }
}
//// Show the list of location to user, prompts the user to enter the location 
async function promptlocation(question) {
    console.log("Please find the list of location");
    console.log(brandElement.location.sort());
    var response = await prompts(question);
    var { locationName } = response;
    location = locationName.toUpperCase();
    if (brandElement.location.indexOf(location) == -1) {
        logger.error("Location prompted by the user is " + location + " and is not found in the supplier configuration file");
        console.log('\x1b[31m%s\x1b[0m', "Please enter the valid Location from the list");
        return await promptlocation(question);
    }
    else {
        logger.info("Location entered by the User :" + location);
        console.log('\x1b[32m%s\x1b[0m', "Location is valid")
        return response;
    }
}
// Show the list of currency to user, prompts the user to enter the currency
async function promptcurrency(question) {
    console.log("Please find the list of Currency");
    console.log(brandElement.currency.sort());
    var response = await prompts(question);
    var { Currency } = response;
    currency = Currency.toUpperCase();
    if (brandElement.currency.indexOf(currency) == -1) {
        logger.error("Currency prompted by the user is " + currency + " and is not found in the supplier configuration file");
        console.log('\x1b[31m%s\x1b[0m', "Please enter the valid Currency from the list");
        return await promptcurrency(question);
    }
    else {
        logger.info("Currency entered by the User :" + location);
        console.log('\x1b[32m%s\x1b[0m', "Currency is valid")
        return response;
    }
}

async function processCsv() {
    const supplier_config = await fs.readJson('./supplierConfig.json')
    const brand_partNumber_config = await fs.readJson('./Brand_partNumber.json')

    supplier_config.forEach(function (element) {
        // console.log(element["config"][0].columns);
        supplier.push(element.supplier);
    });
    const question1 = [
        {
            type: 'text',
            name: 'supplierName',
            message: 'Please enter the supplier Name from the list displayed'
        }
    ]
    var response1 = await promptSupplier(question1);
    var { supplierName } = response1;
    var inputSupplier = supplierName.toUpperCase();
    supplier_config.forEach(function (element) {
        if (inputSupplier == element.supplier) {
            element.config.forEach(function (brandelement) {
                brand.push(brandelement.brand);
            })
        }
    });
    if (brand.length == 1) {
        inputBrand = brand[0];
        logger.info("Brand Name is :" + inputBrand);
        console.log('\x1b[32m%s\x1b[0m', "Brand supplied by the Supplier is " + inputBrand)
    }
    else {
        const question2 = [
            {
                type: 'text',
                name: 'brandName',
                message: 'Please enter the Brand Name from the list displayed'
            }
        ]
        var response2 = await promptBrand(question2);
        var { brandName } = response2;
        inputBrand = brandName.toUpperCase();
    }
    //Validates the brand name in the brand_partNumber config file
    var brandIndex = brand_partNumber_config.findIndex(x => x.brand === inputBrand);
    if (brandIndex == -1) {
        logger.error("Brand Name prompted by the user is " + inputBrand + " and is not found in the brand configuration file");
        console.log('\x1b[31m%s\x1b[0m', "Brand Name entered does not have the minimum part number length in Brand Configuartion file")
    }
    else {
        minimumPartNumberLength = brand_partNumber_config[brandIndex].minimumPartLength
        logger.info("Brand Name is prompted by the User :" + inputBrand + "and minimum part number length is : " + minimumPartNumberLength);
        console.log('\x1b[34m%s\x1b[0m', "Minimum Part Number length for the Brand is :" + minimumPartNumberLength)
    }

    supplier_config.forEach(async function (element) {
        element.config.forEach(async function (brandelement) {
            if (inputSupplier == element.supplier && inputBrand == brandelement.brand) {
                supplierElement = element;
                brandElement = brandelement;
            }
        })
    })


    if (brandElement.location.length == 1) {
        logger.info("Location of the Supplier is " + brandElement.location);
        console.log('\x1b[34m%s\x1b[0m', "Location of the Supplier is " + brandElement.location);
        location = brandElement.location[0];
    }
    else {
        const question3 = [
            {
                type: 'text',
                name: 'locationName',
                message: 'Please enter the location from the list displayed'
            }
        ]
        var response3 = await promptlocation(question3);
        var { locationName } = response3;
        location = locationName.toUpperCase();
    }
    if (brandElement.currency.length == 1) {
        logger.info("Currency of the Supplier is " + brandElement.currency);
        console.log('\x1b[34m%s\x1b[0m', "Currency of the Supplier is " + brandElement.currency);
        currency = brandElement.currency[0];
    }
    else {
        const question4 = [
            {
                type: 'text',
                name: 'Currency',
                message: 'Please enter the currency from the list displayed'
            }
        ]
        var response4 = await promptcurrency(question4);
        var { Currency } = response4;
        currency = Currency.toUpperCase();
    }
    gst = brandElement.gst
    discount = brandElement.discount;
    if (discount) {
        logger.info("The discount used is :" + discount);
        console.log("The discount used is :" + discount);
    }
    coulmnsindex = brandElement.columns;

    if (brandElement.decimalFormat == "Comma") {
        logger.info("Decimal format for the supplier is " + brandElement.decimalFormat);
        console.log("Decimal format for the supplier is " + brandElement.decimalFormat + ". Comma is replaced by dot to represent the decimal place")
    }
    else {
        logger.info("Decimal format for the supplier is Decimal");
        console.log("Decimal format is :Decimal . No replacement is done")
    }
    spliceBrandNamefromPartNumber = brandElement.partNumberSplice;
    logger.info("Process shows the configuration to the user");
    console.log("Please find the configuration used by the process to create the Output file")
    console.table(coulmnsindex);
    var m_names = ['JAN', 'FEB', 'MAR',
        'APR', 'MAY', 'JUN', 'JUL',
        'AUG', 'SEP', 'OCT', 'NOV', 'DEC'];
    d = new Date();
    var month = m_names[d.getMonth()];
    var year = d.getFullYear();

    const jsonArray = await csv({
        noheader: true,
        output: "csv"
    }).fromFile(csvFilePath);
    var jsonCount = 0;
    var isColumnHeaderCheck = false;
    var finalarr = [];
    var sampleArr = [];
    var isPriceAlpha = false;
    var columnHeaders;
    jsonArray.forEach((data, index) => {
        if (brandElement.columnheader) {
            if (data[0] == brandElement.columnheader) {
                isColumnHeaderCheck = true;
                return false;

            }
            if (!isColumnHeaderCheck) {
                return false;

            }
        }
        if (index == 0) {
            columnHeaders = data;
            return false
        }
        var inputPrice = data[Object.keys(data)[coulmnsindex.price - 1]];
        if (/[a-zA-Z]/g.test(inputPrice))                     //Check if the input price has alpha characters
        {
            // console.log(inputPrice);
            isPriceAlpha = true;
            priceAlphaArray.push(data)
        }
    })
    if (isPriceAlpha) {
        logger.error("Price column processed has the alpha character");
        console.log('\x1b[41m%s\x1b[0m', "Price has alpha characters. Please check the input price column");
        console.log("Please find the below input price row, that has the alpha character")
        var priceAlpha = [];
        priceAlphaArray.forEach((priceal) => {
            var obj = {};
            priceal.forEach((element, index) => {
                obj[columnHeaders[index]] = element;
            })
            priceAlpha.push(obj);
        })
        console.table(priceAlpha);
        const question7 = [
            {
                type: 'confirm',
                name: 'priceAlphaChangeConfirm',
                message: 'Please confirm for the replace of the alpha character in the input file '
            }
        ]
        var response7 = await prompts(question7);
        var { priceAlphaChangeConfirm } = response7;
        console.log(priceAlphaChangeConfirm);
        if (!priceAlphaChangeConfirm) {
            logger.error("Process stopped with error of alpha character in the input file")
            console.log("Please check for the configuration or input file");
            return false;
        }
    }

    jsonArray.forEach((data, index) => {
        //Check for the columnheader in the file
        if (brandElement.columnheader) {
            if (data[0] == brandElement.columnheader) {
                isColumnHeaderCheck = true;
                return false;

            }
            if (!isColumnHeaderCheck) {
                return false;

            }
        }
        if (index == 0) {
            columnHeaders = data;
            return false
        }
        var inputPartNumber = data[Object.keys(data)[coulmnsindex.partNumber - 1]];
        var description = data[Object.keys(data)[coulmnsindex.description - 1]];
        var inputFormerPartNumber = data[Object.keys(data)[coulmnsindex.formerPartNumber - 1]];
        var inputSupersedePartNumber = data[Object.keys(data)[coulmnsindex.supersedePartNumber - 1]];
        var inputPrice = data[Object.keys(data)[coulmnsindex.price - 1]];
        var inputGST = data[Object.keys(data)[coulmnsindex.gst - 1]];
        var obj = {};
        obj["Brand"] = inputBrand;
        obj["Supplier Name"] = inputSupplier + "_" + currency + "_" + month + "_" + year + "_" + location;
        obj["Location"] = location;
        obj["Currency"] = currency;
        obj["Part Number"] = processPartnumber(inputPartNumber, index);
        obj["Description"] = description ? description : '';
        obj["FORMER PN"] = processPartnumber(inputFormerPartNumber);
        obj["SUPERSESSION"] = processPartnumber(inputSupersedePartNumber);
        //Input price conversion

        if (inputPrice) {
            if (/[a-zA-Z]/g.test(inputPrice))                     //Check if the input price has alpha characters
            {
                // console.log(inputPrice)
                obj["Price"] = 0;

            }
            else {
                obj["Price"] = inputPrice.replace(/[^0-9.,]/gi, ''); //Replace the input price to have only numbers,decimal,comma
                if (brandElement.decimalFormat == "Comma")              //Checks for the decimal format to comma
                {
                    obj["Price"] = obj["Price"].replace(/,([^,]*)$/, ".$1"); //Replace the last comma to dot to represent decimal place
                }
                obj["Price"] = obj["Price"].replace(/[^0-9.]/gi, '');     //Replace the input price to have only numbers,decimal point
                obj["Price"] = parseFloat(obj["Price"]).toFixed(2);     // Fix the decimal points to 2
                if (coulmnsindex.gst)                                   //Checks for the gst configuration
                {
                    if (!inputGST)                                      //Checks for the gst in the input price file
                    {
                        isgstempty = true;
                        gstNaArray.push(data);
                    }
                    else {
                        if (/[a-zA-Z]/g.test(inputGST))                     //Check if the gst is valid
                        {
                            gstNAN = true;
                            gstNaNArray.push(data);
                        }
                        else {
                            inputGST = inputGST.replace(/[^0-9.]/gi, '');   //Removes the percentage symbol in the input value
                            var inputgstdecimal = inputGST / 100;             //Converts the percentage to decimal
                            obj["Price"] = (obj["Price"] / (1 + inputgstdecimal)).toFixed(2);   //Deducts the gst from the input price
                        }
                    }
                }
                if (discount)                         //Checks for the discount configuration
                {
                    obj["Price"] = (obj["Price"] - ((discount * obj["Price"]) / 100)).toFixed(2); //Deducts the discount from the price of previous step and fix decimal point to 2
                }
            }
        }

        else {
            priceEmptyCount++;

            priceNaArray.push(data);

        }
        if (!inputPartNumber) {
            partNumberEmptyCount++;
            partNumberNAarray.push(data);
        }

        if (inputPartNumber)                             // Check for the input part number
        {
            jsonCount++;
            if (inputPrice) {
                if (obj["Price"]) {
                    // console.log(1,obj["Price"])
                    obj["Price"] = Number(obj["Price"])
                }
            } else {
                // console.log(obj["Price"])
                obj["Price"] = 0;
            }
            finalarr.push(obj);                                       // Creates the final array 
            if (jsonCount < 12) {
                sampleArr.push(obj)                                  // Creates the sample array
            }
        }
    })
    if (partNumberEmptyCount) {
        console.log("Count of the empty part number rows :" + partNumberEmptyCount + ". The rows are not added in the Output file");
        logger.error("Input file has empty part number rows");
        var finalPartNumberNa = [];
        partNumberNAarray.forEach((partnumberemptyarray) => {
            var obj = {};
            partnumberemptyarray.forEach((element, index) => {
                obj[columnHeaders[index]] = element;
            })
            finalPartNumberNa.push(obj);
        })
        console.table(finalPartNumberNa);
        logger.info(finalPartNumberNa);

    }
    if (priceEmptyCount) {
        logger.error("Input file has empty price rows");
        console.log("Count of the empty price rows :" + priceEmptyCount + ". The below rows has been added to the output file without price");
        var finalpriceNa = [];
        priceNaArray.forEach((singleprice, dataindex) => {
            var obj = {};
            singleprice.forEach((element, index) => {
                obj[columnHeaders[index]] = element;
            })
            if (dataindex < 15) {
                finalpriceNa.push(obj);
            }
        })

        console.table(finalpriceNa);

    }
    //Exception for columnheader
    if (brandElement.columnheader && !isColumnHeaderCheck) {
        logger.error("Process does not find the column header as per configuartion file");
        console.log('\x1b[41m%s\x1b[0m', "Process does not find the column header as per configuartion file");
        return false;
    }
    //Exception for gst value empty
    if (isgstempty) {
        logger.error("GST percentage is empty");
        console.log('\x1b[41m%s\x1b[0m', "GST percentage is empty in the input price file");
        console.log("Please find the below input price row, that has the empty gst value")
        var gstEmpty = [];
        gstNaArray.forEach((gstEmptyAr) => {
            var obj = {};
            gstEmptyAr.forEach((element, index) => {
                obj[columnHeaders[index]] = element;
            })
            gstEmpty.push(obj);
        })
        console.table(gstEmpty);
        return false;
    }
    if (gstNAN) {
        logger.error("GST percentage is invalid");
        console.log('\x1b[41m%s\x1b[0m', "GST percentage is invalid in the input price file");
        console.log("Please find the below input row having invaild GST")
        var gstNANAr = [];
        gstNaNArray.forEach((gstNAN) => {
            var obj = {};
            gstNAN.forEach((element, index) => {
                obj[columnHeaders[index]] = element;
            })
            gstNANAr.push(obj);
        })
        console.table(gstNANAr);
        return false;
    }

    logger.info("Process shows the sample output to the user");
    console.log("Please find the Sample Output as per configuration")
    console.table(sampleArr);
    const question6 = [
        {
            type: 'confirm',
            name: 'inputConfirm',
            message: 'Please confirm for the Ouptut file to be generated: '
        }
    ]
    var response6 = await prompts(question6);
    var { inputConfirm } = response6;
    console.log(inputConfirm);
    //Write the final array to the output file 
    if (inputConfirm == true) {
        var outputFileName = inputBrand + "_" + inputSupplier + "_" + currency + "_" + location + "_" + month + "_" + year;
        const output_csv = await convert.json2csv(finalarr);
        var folderName = "./Output/" + inputBrand;
        if (!fs.existsSync(folderName)) {
            fs.mkdirSync(folderName);
        }
        fs.writeFileSync(folderName + "/" + outputFileName + '.csv', output_csv)
        console.log('\x1b[42m%s\x1b[0m', "Process executed.Output file created in the Output Folder");
        logger.info("Process created the output file successfully");
    }
    else {
        logger.error("User has prompted not to create the output file. User has to check for the input file columns or configuratin file ");
        console.log("Please check for the input file or configuration file");
        return false;
    }
}
// part number conversion
function processPartnumber(partnumber, indexdata = null) {
    if (partnumber) {
        if (brandElement.partNumberSplice)      // Check for the part number splice in the configuration file
        {
            partnumber = partnumber.toString().substring(spliceBrandNamefromPartNumber); // removes the prefix added to the part number
        }
        partnumber = partnumber.replace(/[^a-z0-9]/gi, '');  //removes the special characters,spaces

        partnumber = partnumber.padStart(minimumPartNumberLength, 0); //pad zeros prefix to the partnumber as per brand_partnumber configuration
        partnumber = partnumber.toUpperCase();        //converts the part number to upper case.
        return partnumber
    }
    else {

        return "";
    }
}
processCsv()
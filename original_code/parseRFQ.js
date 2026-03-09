const fs = require('fs-extra');
var prompts = require('prompts');
const csv = require('csvtojson');
const convert = require('json-2-csv');
var csvFilePath = "";
const dirCont = fs.readdirSync("./RFQ/");
var samplearrconvert=[];
var samplearrcustomer=[];
const csvCount = dirCont.filter( ( elm ) => elm.match(/.*\.(csv?)/ig)).length;
if(csvCount>1){
    console.log('\x1b[31m%s\x1b[0m',"Input folder has multiple files")
    return false;
}
fs.readdirSync("./RFQ/").forEach(file => {
    if(file.match(/.*\.(csv?)/ig)){
        csvFilePath = "./RFQ/" + file;
    }
});

if (!csvFilePath) {
    console.log('\x1b[31m%s\x1b[0m', "Please add the input file!");
    return false;
}
var minimumPartNumberLength;
var brand = [];
var inputBrand;
async function promptBrand(question) {
    console.log("Please find the list of Brands");
    console.log(brand.sort());
    var response = await prompts(question);
    var { brandName } = response;
    inputBrand = brandName.toUpperCase();
    if (brand.indexOf(inputBrand) == -1) {
        console.log('\x1b[31m%s\x1b[0m', "Please enter the valid Brand Name from the list");
        return await promptBrand(question);
    } else {
        console.log('\x1b[32m%s\x1b[0m',"Brand Name is valid")
        return response;
    }
}

async function processCsv() {
    const brand_partNumber_config = await fs.readJson('./Brand_partNumber.json')
    brand_partNumber_config.forEach(function (element) {
        // console.log(element["config"][0].columns);
        brand.push(element.brand);
    });
   
      const question1 = [
            {
                type: 'text',
                name: 'brandName',
                message: 'Please enter the Brand Name from the list displayed'
            }
        ]
        var response1 = await promptBrand(question1);
        var { brandName } = response1;
        inputBrand = brandName.toUpperCase();
        var brandIndex = brand_partNumber_config.findIndex(x => x.brand === inputBrand);
        if (brandIndex == -1) {
            console.log('\x1b[31m%s\x1b[0m', "Brand Name entered does not have the minimum part number length in Brand Configuartion file")
        }
        else {
            minimumPartNumberLength = brand_partNumber_config[brandIndex].minimumPartLength
            console.log('\x1b[34m%s\x1b[0m',"Minimum Part Number length for the Brand is :" + minimumPartNumberLength)
        }
        
        const question2 = [
            {
                type: 'text',
                name: 'RFQID',
                message: 'Please enter the RFQ ID for the output file name'
            }
        ]
        var response2 = await prompts(question2);
        var { RFQID } = response2;
        inputRFQID = RFQID.toUpperCase();
    
        const jsonArray = await csv({
            noheader:true,
            output: "csv"
        }).fromFile(csvFilePath);
        var finalarrconvert = [];
        var finalarrcustomer = [];
       

        jsonArray.forEach((data, index) => {
            if(index == 0){
                return false
            }
            var inputPartNumber = data[0];

            var objconvert = {};
            var objcustomer={};
            objconvert["Brand"] = inputBrand;
            objconvert["Part Number"] = processPartnumber(inputPartNumber, index);
            objcustomer["Part Number"] = objconvert["Part Number"];
            objcustomer["Original Part Number"] = data[0];
            // console.log(typeof(data[0]));
            if (inputPartNumber) {
                finalarrconvert.push(objconvert);
                finalarrcustomer.push(objcustomer);
                
                
            }
            if(index<12)
            {
                samplearrconvert.push(objconvert);
                samplearrcustomer.push(objcustomer);

            }
           
        })
        console.table(samplearrconvert);
        console.table(samplearrcustomer);
       
        var outputFileName1 = inputRFQID + "_" + inputBrand ;
            const output_csv1 = await convert.json2csv(finalarrconvert);
            var outputFileName2 = inputRFQID + "_" + "_CUSTOMERPARTMAP" ;
            const output_csv2 = await convert.json2csv(finalarrcustomer);
            var folderName="./Output_RFQ/";
            if (!fs.existsSync(folderName)){
                fs.mkdirSync(folderName);
            }
            fs.writeFileSync(folderName + "/" + outputFileName1 + '.csv', output_csv1)
            fs.writeFileSync(folderName + "/" + outputFileName2 + '.csv', output_csv2)
            console.log('\x1b[42m%s\x1b[0m',"Process executed.Output files created in the Output Folder");
       
        }
function processPartnumber(partnumber, indexdata = null) {
    if (partnumber) {
        
        // if(brandElement.partNumberSplice){
        //     partnumber = partnumber.toString().substring(spliceBrandNamefromPartNumber);
        // }
        partnumber = partnumber.replace(/[^a-z0-9]/gi, '');
        partnumber = partnumber.padStart(minimumPartNumberLength, 0);
        partnumber=partnumber.toUpperCase();
        // console.log(typeof(partnumber))

        return partnumber
    }
    else {
        return "";
    }
}
processCsv()

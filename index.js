const aws = require("aws-sdk"),
    AthenaExpress = require("athena-express");

const awsCredentials = {
    region: "us-east-1"
};
aws.config.update(awsCredentials);

const athenaExpressConfig = { aws, s3: "s3://athena-express-akiavslbgx-2021", getStats: true };
const athenaExpress = new AthenaExpress(athenaExpressConfig);

exports.handler = async function(event, context) {
    // Parse the input for the name, city, time and day property values
    if (event && event.queryStringParameters && event.queryStringParameters.severity && event.queryStringParameters.accident_date && event.queryStringParameters.lga_name_all) {
        let severity = decodeURI(event.queryStringParameters.severity);
        let accident_date = decodeURI(event.queryStringParameters.accident_date);
        let lga_name_all = decodeURI(event.queryStringParameters.lga_name_all);
        console.log("data", severity, accident_date, lga_name_all);
        const query = `SELECT accident_no,
                          latitude,
                          longitude,
                          severity,
                          lga_name_all,
                          road_geometry,
                          accident_type
                   FROM data.crashes_record where lga_name_all = '${lga_name_all}' and severity = '${severity}' and accident_date like '%${accident_date}' limit 2000`
        try {
            console.log(query)
            let data = await athenaExpress.query(query);
            const response = {
                "statusCode": 200,
                "headers": {
                    "X-Requested-With": '*',
                    "Access-Control-Allow-Headers": 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,x-requested-with',
                    "Access-Control-Allow-Origin": '*',
                    "Access-Control-Allow-Methods": 'POST,GET,OPTIONS'
                },
                "body": JSON.stringify({ "data": data.Items })
            }
            return response;
        }
        catch (e) {
            console.error(e)
            throw e;
        }
    }
    else if (event && event.queryStringParameters && event.queryStringParameters.table) {
        const query = `SELECT accident_no,
                          latitude,
                          longitude,
                          severity,
                          lga_name_all,
                          road_geometry,
                          accident_type,
                          accident_date
                   FROM data.crashes_record limit 200`
        try {
            console.log(query)
            let data = await athenaExpress.query(query);
            const response = {
                "statusCode": 200,
                "headers": {
                    "X-Requested-With": '*',
                    "Access-Control-Allow-Headers": 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,x-requested-with',
                    "Access-Control-Allow-Origin": '*',
                    "Access-Control-Allow-Methods": 'POST,GET,OPTIONS'
                },
                "body": JSON.stringify({ "data": data.Items })
            }
            return response;
        }
        catch (e) {
            console.error(e)
            throw e;
        }

    }
    else if (event && event.queryStringParameters && event.queryStringParameters.export) {
        try {
            const s3 = new aws.S3();
            const params = {
                Bucket: "forexport",
                Key: 'Crashes_Last_Five_Years.csv', //the directory in S3
                Expires: 60
            }

            try {
                const url = await new Promise((resolve, reject) => {
                    s3.getSignedUrl('getObject', params, function(err, url) {
                        if (err) {
                            reject(err)
                        }
                        resolve(url)
                    })
                })
                const response = {
                    "statusCode": 200,
                    "headers": {
                        "X-Requested-With": '*',
                        "Access-Control-Allow-Headers": 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,x-requested-with',
                        "Access-Control-Allow-Origin": '*',
                        "Access-Control-Allow-Methods": 'POST,GET,OPTIONS'
                    },
                    "body": JSON.stringify({ "data": url })
                }
                return response;
            }
            catch (err) {
                console.error('s3 getObject,  get signedUrl failed')
                throw err
            }
        }
        catch (error) {
            console.log(error);
            return;
        }
    }

};

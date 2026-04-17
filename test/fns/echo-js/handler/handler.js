'use strict';

// echo-js
// This function returns the request body and headers as a JSON object.

const GATEWAY_BASE = (process.env.FAASD_GATEWAY_URL || "http://127.0.0.1:8080").replace(/\/$/, "");

module.exports = async (request, context) => {
    console.log("Body:", request.body);
    console.log("Headers:", request.headers);
    return context.status(200).succeed({
        body: request.body,
        headers: request.headers,
    });
};

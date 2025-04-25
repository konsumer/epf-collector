// this just grabs the initial collection (full dumps)
// normally you would also do incremental once a week

import { writeFile, stat, mkdir } from "node:fs/promises";
import * as cheerio from "cheerio";

const exists = async (f) => {
    try {
        await stat(f);
        return true;
    } catch (e) {
        return false;
    }
};

const { EPF_USERNAME, EPF_PASSWORD } = process.env;

if (!EPF_USERNAME || !EPF_PASSWORD) {
    console.error("EPF_USERNAME && EPF_PASSWORD are required.");
    process.exit(1);
}

// Get an EPF url
const get = (u) =>
    fetch(`https://feeds.itunes.apple.com/feeds/epf/${u}`, {
        headers: {
            Authorization: "Basic " + btoa(EPF_USERNAME + ":" + EPF_PASSWORD),
        },
    });

// get current list of tbz's
export async function getList(u = "v5/current/") {
    const out = [];
    const $ = cheerio.load(await get(u).then((r) => r.text()));
    for (const a of $("a")) {
        if (!a.attribs.href.startsWith("?")) {
            out.push(a.attribs.href);
        }
    }
    return out;
}

// get all the full dumps
for (const collection of await getList()) {
    if (collection !== "incremental/") {
        const files = await getList(`v5/current/${collection}`);
        console.log(collection);
        for (const file of files) {
            const fe = await exists(`data/${collection}${file}`);
            try {
                await mkdir(`data/${collection}`, { recursive: true });
            } catch (e) {}
            console.log(
                `  data/${collection}${file}: ${fe ? "exists" : "downloading"}`,
            );
            if (!fe) {
                const bytes = await get(`v5/current/${collection}${file}`).then(
                    (r) => r.arrayBuffer(),
                );
                await writeFile(
                    `data/${collection}${file}`,
                    new Uint8Array(bytes),
                );
            }
        }
    }
}

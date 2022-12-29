
import snowflakePlugin,
{
    waitFor,
    timeout,
    retryPromiseWithDelayAndTimeout,
    generateCsvString,
    Snowflake
} from "./index"
import AWS, { S3 } from 'aws-sdk'
import Redis from 'ioredis'
import { v4 as uuid4 } from "uuid"
import { setupServer } from "msw/node"
import { rest } from "msw"
import zlib from "zlib"
import axios from "axios"
import MockAdapter from "axios-mock-adapter"

// Set this higher for now to test the retry logic
jest.setTimeout(60000)

test("waitFor actually waits", async () => {
    const start = Date.now()
    await waitFor(500)
    const end = Date.now()
    expect(end - start).toBeGreaterThanOrEqual(500)
})

test("timeout actually times out", async () => {
    await expect(timeout(new Promise(async (resolve, reject) => {
        await waitFor(1000)
        resolve("This should time out first")
    }), 500, "timed out")).rejects.toEqual("timed out")
})

test("retryPromiseWithDelayAndTimeout actually retries with delay and timeout", async () => {
    const start = Date.now()
    let retries = 0
    await expect(retryPromiseWithDelayAndTimeout(() => {
        return new Promise(async (resolve, reject) => {
            retries += 1
            await waitFor(100)
        })
    }, 5, 100, 50)).rejects.toEqual("Promise timed out")
    const end = Date.now()
    expect(end - start).toBeGreaterThanOrEqual(500)
    expect(retries).toEqual(5)
})

test("generateCsvString generates expected csv", () => {
    const events = [
        {
            event: "some",
            distinct_id: "123",
            team_id: 1,
            ip: "0.0.0.0",
            site_url: "https://app.posthog.com",
            timestamp: "2020-01-01T01:01:01Z",
            uuid: "9ae018e7-f87f-47bd-9ea1-63431d9fc071",
            properties: JSON.stringify({ some: "property" }),
            elements: JSON.stringify([{ some: "element" }]),
            people_set: JSON.stringify({ some: "people_set" }),
            people_set_once: JSON.stringify({ some: "people_set_once" }),
        },
        {
            event: "some",
            distinct_id: "123",
            team_id: 1,
            ip: "0.0.0.0",
            site_url: "https://app.posthog.com",
            timestamp: "2020-01-01T01:01:01Z",
            uuid: "76c70c78-5f98-4153-a309-65fa920d35e7",
            properties: JSON.stringify({ some: "property" }),
            elements: JSON.stringify([{ some: "element" }]),
            people_set: JSON.stringify({ some: "people_set" }),
            people_set_once: JSON.stringify({ some: "people_set_once" }),
        },
        {
            event: "some",
            distinct_id: "123",
            team_id: 1,
            ip: "0.0.0.0",
            site_url: "https://app.posthog.com",
            timestamp: "2020-01-01T01:01:01Z",
            uuid: "76c70c78-5f98-4153-a309-65fuk20d35e7",
            properties: JSON.stringify({ some: "property" }),
            elements: JSON.stringify([{ some: "element" }]),
            people_set: JSON.stringify({ some: "people_set" }),
            people_set_once: JSON.stringify({ some: "people_set_once" }),
        }
    ]
    const csv = generateCsvString(events)
    expect(csv).toEqual(
        `uuid|$|event|$|properties|$|elements|$|people_set|$|people_set_once|$|distinct_id|$|team_id|$|ip|$|site_url|$|timestamp\n`
        + `9ae018e7-f87f-47bd-9ea1-63431d9fc071|$|some|$|{"some":"property"}|$|[{"some":"element"}]|$|{"some":"people_set"}|$|{"some":"people_set_once"}|$|123|$|1|$|0.0.0.0|$|https://app.posthog.com|$|2020-01-01T01:01:01Z\n`
        + `76c70c78-5f98-4153-a309-65fa920d35e7|$|some|$|{"some":"property"}|$|[{"some":"element"}]|$|{"some":"people_set"}|$|{"some":"people_set_once"}|$|123|$|1|$|0.0.0.0|$|https://app.posthog.com|$|2020-01-01T01:01:01Z\n`
        + `76c70c78-5f98-4153-a309-65fuk20d35e7|$|some|$|{"some":"property"}|$|[{"some":"element"}]|$|{"some":"people_set"}|$|{"some":"people_set_once"}|$|123|$|1|$|0.0.0.0|$|https://app.posthog.com|$|2020-01-01T01:01:01Z`
    )
})

test("handles bad connection", async () => {
    mswServer.close()
    const mock = new MockAdapter(axios)
    mock.onPost('/session/v2/login-request').reply(500, "Internal Server Error")

    // NOTE: we create random names for tests such that we can run tests
    // concurrently without fear of conflicts
    const bucketName = uuid4()
    const snowflakeAccount = uuid4()

    let meta = {}
    Object.assign(meta, {
        attachments: {},
        config: {
            account: snowflakeAccount,
            username: "username",
            password: "password",
            database: "database",
            dbschema: "dbschema",
            table: "table",
            stage: "S4",
            eventsToIgnore: "eventsToIgnore",
            bucketName: bucketName,
            warehouse: "warehouse",
            awsAccessKeyId: "awsAccessKeyId",
            awsSecretAccessKey: "awsSecretAccessKey",
            awsRegion: "string",
            storageIntegrationName: "storageIntegrationName",
            role: "role",
            stageToUse: 'S4' as const,
            purgeFromStage: 'Yes' as const,
            bucketPath: "bucketPath",
            retryCopyIntoOperations: 'Yes' as const,
            forceCopy: 'Yes' as const,
            debug: 'ON' as const,
        },
        jobs: createJobs(snowflakePlugin.jobs)(meta),
        cache: cache,
        storage: storage,
        // Cast to any, as otherwise we don't match plugin call signatures
        global: {} as any,
        geoip: {} as any
    })

    await snowflakePlugin.setupPlugin?.(meta)
    mock.restore();
})

test("handles events", async () => {
    // Checks for the happy path
    //
    // TODO: check for:
    //
    //  1. snowflake retry functionality
    //  2. s3 failure cases
    //  3. what happens if [workhouse is suspended](https://posthogusers.slack.com/archives/C01GLBKHKQT/p1650526998274619?thread_ts=1649761835.322489&cid=C01GLBKHKQT)

    AWS.config.update({
        accessKeyId: "awsAccessKeyId",
        secretAccessKey: "awsSecretAccessKey",
        region: "us-east-1",
        s3ForcePathStyle: true,
        s3: {
            endpoint: 'http://localhost:4566'
        }
    })

    // NOTE: we create random names for tests such that we can run tests
    // concurrently without fear of conflicts
    const bucketName = uuid4()
    const snowflakeAccount = uuid4()

    const s3 = new S3()
    await s3.createBucket({ Bucket: bucketName }).promise()

    let meta = {}
    Object.assign(meta, {
        attachments: {},
        config: {
            account: snowflakeAccount,
            username: "username",
            password: "password",
            database: "database",
            dbschema: "dbschema",
            table: "table",
            stage: "S3",
            eventsToIgnore: "eventsToIgnore",
            bucketName: bucketName,
            warehouse: "warehouse",
            awsAccessKeyId: "awsAccessKeyId",
            awsSecretAccessKey: "awsSecretAccessKey",
            awsRegion: "string",
            storageIntegrationName: "storageIntegrationName",
            role: "role",
            stageToUse: 'S3' as const,
            purgeFromStage: 'Yes' as const,
            bucketPath: "bucketPath",
            retryCopyIntoOperations: 'Yes' as const,
            forceCopy: 'Yes' as const,
            debug: 'ON' as const,
        },
        jobs: createJobs(snowflakePlugin.jobs)(meta),
        cache: cache,
        storage: storage,
        // Cast to any, as otherwise we don't match plugin call signatures
        global: {} as any,
        geoip: {} as any
    })

    const events = [
        {
            event: "some",
            distinct_id: "123",
            ip: "10.10.10.10",
            site_url: "https://app.posthog.com",
            team_id: 1,
            now: "2020-01-01T01:01:01Z"
        },
        {
            event: "events",
            distinct_id: "456",
            ip: "10.10.10.10",
            site_url: "https://app.posthog.com",
            team_id: 1,
            now: "2020-01-01T01:01:01Z"
        },
        {
            event: "$autocapture",
            distinct_id: "autocapture",
            ip: "10.10.10.10",
            site_url: "https://app.posthog.com",
            team_id: 1,
            now: "2020-01-01T01:01:01Z",
            properties: {},
            elements: [{ some: "element" }]
        }
    ]
    const db = createSnowflakeMock(snowflakeAccount)

    await snowflakePlugin.setupPlugin?.(meta)

    for (let i = 0; i < 3; i++) { // to have >1 files to copy over
        await cache.expire('lastRun', 0)
        await snowflakePlugin.exportEvents?.([events[i]], meta)
    }

    await snowflakePlugin.runEveryMinute?.(meta)
    await snowflakePlugin.teardownPlugin?.(meta)

    const s3Keys = (await s3.listObjects({ Bucket: bucketName }).promise()).Contents?.map((obj) => obj.Key) || []
    expect(s3Keys.length).toEqual(3)

    // Snowflake gets the right files
    const filesLists = db.queries.map(query => /FILES = (?<files>.*)/m.exec(query)?.groups.files).filter(Boolean)
    const copiedFiles = filesLists.map(files => files.split("'").filter(file => file.includes("csv"))).flat()
    expect(copiedFiles.sort()).toEqual(s3Keys.sort())

    // The content in S3 is what we expect
    const csvStrings = await Promise.all(s3Keys.map(async s3Key => {
        const response = await s3.getObject({ Bucket: bucketName, Key: s3Key }).promise()
        return (response.Body || "").toString('utf8')
    }))

    const columns = [
        'uuid',
        'event',
        'properties',
        'elements',
        'people_set',
        'people_set_once',
        'distinct_id',
        'team_id',
        'ip',
        'site_url',
        'timestamp',
    ]

    // Get just the data rows, ignoring the header row
    const cvsRows = csvStrings.sort().flatMap(csvString => csvString.split("\n").slice(1))

    const exportedEvents = cvsRows.map(row =>
        Object.fromEntries(row.split("|$|").map((value, index) => [columns[index], value]))
    )

    expect(exportedEvents).toEqual([
        {
            "distinct_id": "autocapture",
            "elements": "[{\"some\":\"element\"}]",
            "event": "$autocapture",
            "ip": "10.10.10.10",
            "people_set": "{}",
            "people_set_once": "{}",
            "properties": "{}",
            "site_url": "https://app.posthog.com",
            "team_id": "1",
            "timestamp": "2020-01-01T01:01:01Z",
            "uuid": "",
        },
        {
            "distinct_id": "456",
            "elements": "[]",
            "event": "events",
            "ip": "10.10.10.10",
            "people_set": "{}",
            "people_set_once": "{}",
            "properties": "{}",
            "site_url": "https://app.posthog.com",
            "team_id": "1",
            "timestamp": "2020-01-01T01:01:01Z",
            "uuid": "",
        },
        {
            "distinct_id": "123",
            "elements": "[]",
            "event": "some",
            "ip": "10.10.10.10",
            "people_set": "{}",
            "people_set_once": "{}",
            "properties": "{}",
            "site_url": "https://app.posthog.com",
            "team_id": "1",
            "timestamp": "2020-01-01T01:01:01Z",
            "uuid": "",
        },
    ])
})

test("handles > 1k files", async () => {
    // NOTE: we create random names for tests such that we can run tests
    // concurrently without fear of conflicts
    const bucketName = uuid4()
    const snowflakeAccount = uuid4()

    const snowflakeMock = jest.fn()
    let meta = {}
    Object.assign(meta, {
        attachments: {}, config: {
            account: snowflakeAccount,
            username: "username",
            password: "password",
            database: "database",
            dbschema: "dbschema",
            table: "table",
            stage: "S3",
            eventsToIgnore: "eventsToIgnore",
            bucketName: bucketName,
            warehouse: "warehouse",
            awsAccessKeyId: "awsAccessKeyId",
            awsSecretAccessKey: "awsSecretAccessKey",
            awsRegion: "string",
            storageIntegrationName: "storageIntegrationName",
            role: "role",
            stageToUse: 'S3' as const,
            purgeFromStage: 'Yes' as const,
            bucketPath: "bucketPath",
            retryCopyIntoOperations: 'Yes' as const,
            forceCopy: 'Yes' as const,
            debug: 'ON' as const,
        },
        jobs: createJobs(snowflakePlugin.jobs)(meta),
        cache: cache,
        storage: storage,
        // Cast to any, as otherwise we don't match plugin call signatures
        global: { snowflake: { copyIntoTableFromStage: snowflakeMock } } as any,
        geoip: {} as any
    })

    await storage.set('_files_staged_for_copy_into_snowflake', Array(2100).fill('file'))
    await cache.expire('lastRun', 0)
    await snowflakePlugin.runEveryMinute(meta)

    expect(snowflakeMock.mock.calls.length).toBe(42)

})

const createJob = (job: (payload, meta) => null) => (meta: any) => (payload: any) => ({
    runIn: async (runIn: number, unit: string) => {
        await new Promise((resolve) => setTimeout(resolve, runIn))
        await job(payload, meta)
    }
})

const createJobs = (jobs: any) => (meta: any) => Object.fromEntries(Object.entries(jobs).map(([jobName, jobFn]) => [jobName, createJob(jobFn)(meta)]))


// Use fake timers so we can better control e.g. backoff/retry code.
// Use legacy fake timers. With modern timers there seems to be little feedback
// on fails due to test timeouts.
beforeEach(() => {
    jest.useFakeTimers({ advanceTimers: 30 })
})

afterEach(() => {
    jest.runOnlyPendingTimers()
    jest.clearAllTimers()
    jest.useRealTimers()
})

// Create something that looks like the expected cache interface. Note it only
// differs by the addition of the `defaultValue` argument.
// Redis is required to handle staging(?) of S3 files to be pushed to snowflake
let redis: Redis | undefined;
let cache: any
let storage: any
let mockStorage: Map<string, unknown>

beforeAll(() => {
    redis = new Redis()

    cache = {
        lpush: redis.lpush.bind(redis),
        llen: redis.llen.bind(redis),
        lrange: redis.lrange.bind(redis),
        set: redis.set.bind(redis),
        expire: redis.expire.bind(redis),
        get: (key: string, defaultValue: unknown) => redis.get(key)
    }

    mockStorage = new Map()
    storage = {
        // Based of https://github.com/PostHog/posthog/blob/master/plugin-server/src/worker/vm/extensions/storage.ts
        get: async function (key: string, defaultValue: unknown): Promise<unknown> {
            await Promise.resolve()
            if (mockStorage.has(key)) {
                const res = mockStorage.get(key)
                if (res) {
                    return JSON.parse(String(res))
                }
            }
            return defaultValue
        },
        set: async function (key: string, value: unknown): Promise<void> {
            await Promise.resolve()
            if (typeof value === 'undefined') {
                mockStorage.delete(key)
            } else {
                mockStorage.set(key, JSON.stringify(value))
            }
        },
        del: async function (key: string): Promise<void> {
            await Promise.resolve()
            mockStorage.delete(key)
        },
    }

})

afterAll(() => {
    redis.quit()
})


// Setup Snowflake MSW service
const mswServer = setupServer()

beforeAll(() => {
    mswServer.listen()
})

afterAll(() => {
    mswServer.close()
})

const createSnowflakeMock = (accountName: string) => {
    // Create something that kind of looks like snowflake, albeit not
    // functional.
    const baseUri = `https://${accountName}.snowflakecomputing.com`

    const db = { queries: [] }

    mswServer.use(
        // Before making queries, we need to login via username/password and get
        // a token we can use for subsequent auth requests.
        rest.post(`${baseUri}/session/v1/login-request`, (req, res, ctx) => {
            return res(ctx.json({
                "data": {
                    "token": "token",
                },
                "code": null,
                "message": null,
                "success": true
            }))
        }),
        // The API seems to follow this pattern:
        //
        //  1. POST your SQL query up to the API, the resulting resource
        //     identified via a query id. However, we don't actually need to do
        //     anything explicitly with this id, rather we...
        //  2. use the getResultUrl to fetch the results of the query
        //
        // TODO: handle case when query isn't complete on requesting getResultUrl
        rest.post(`${baseUri}/queries/v1/query-request`, async (req, res, ctx) => {
            const queryId = uuid4()
            // snowflake-sdk encodes the request body as gzip, which we recieve
            // in this handler as a stringified hex sequence.
            const requestJson = await new Promise(
                (resolve, reject) => {
                    zlib.gunzip(Buffer.from(req.body, 'hex'), (err, uncompressed) => resolve(uncompressed))
                }
            )
            const request = JSON.parse(requestJson)
            db.queries.push(request.sqlText)
            return res(ctx.json({
                "data": {
                    "getResultUrl": `/queries/${queryId}/result`,
                },
                "code": "333334",
                "message": null,
                "success": true
            }))
        }),
        rest.get(`${baseUri}/queries/:queryId/result`, (req, res, ctx) => {
            return res(ctx.json({
                "data": {
                    "parameters": [],
                    "rowtype": [],
                    "rowset": [],
                    "total": 0,
                    "returned": 0,
                    "queryId": "query-id",
                    "queryResultFormat": "json"
                },
                "code": null,
                "message": null,
                "success": true
            }))
        }),
        // Finally we need to invalidate the authn token by calling logout
        rest.post(`${baseUri}/session/logout-request`, (req, res, ctx) => {
            return res(ctx.status(200))
        }),
    )

    return db;
}

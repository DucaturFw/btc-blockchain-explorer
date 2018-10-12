console.log('starting grabber')

let BTC_NODE = process.env.BTC_EXPLORER_BTC_NODE || ""
let DB_NAME = process.env.BTC_EXPLORER_DB_NAME || ""
let TABLE_BLOCKS = process.env.BTC_EXPLORER_TABLE_BLOCKS || "btc_blocks"
let TABLE_TXS = process.env.BTC_EXPLORER_TABLE_TXS || "btc_txs"
let RETHINK_URI = process.env.BTC_EXPLORER_RETHINK || ""

console.assert(BTC_NODE, "please provide $BTC_EXPLORER_BTC_NODE!")
console.assert(DB_NAME, "please provide $BTC_EXPLORER_DB_NAME!")
console.assert(TABLE_BLOCKS, "please provide $BTC_EXPLORER_TABLE_BLOCKS!")
console.assert(TABLE_TXS, "please provide $BTC_EXPLORER_TABLE_TXS!")

console.log(`btc node: ${BTC_NODE}\ndb name: ${DB_NAME}\nblocks table: ${TABLE_BLOCKS}\ntxs table: ${TABLE_TXS}\nrethink: ${RETHINK_URI}`)

interface IBlock {
	hash: string;
	confirmations: number;
	strippedsize: number;
	size: number;
	weight: number;
	height: number;
	version: number;
	versionHex: string;
	merkleroot: string;
	tx: Array<string>;
	time: number;
	mediantime: number;
	nonce: number;
	bits: string;
	difficulty: number;
	chainwork: string;
	nTx: number;
	previousblockhash: string;
	nextblockhash: string;
}

interface ITransactionScriptPubKey {
	asm: string;
	hex: string;
	reqSigs: number;
	type: string;
	addresses: Array<string>;
}

interface ITransactionInput {
	coinbase: string;
	sequence: number;
}

interface ITransactionOutput {
	value: number;
	n: number;
	scriptPubKey: ITransactionScriptPubKey;
}

interface ITransaction {
	txid: string;
	hash: string;
	version: number;
	size: number;
	vsize: number;
	locktime: number;
	vin: Array<ITransactionInput>;
	vout: Array<ITransactionOutput>;
	hex: string;
	blockhash: string;
	confirmations: number;
	time: number;
	blocktime: number;
}

import axios from "axios"
import r from "rethinkdb"

let api = <T>(method: string, params: any = []) => axios
	.post(BTC_NODE, { jsonrpc: "1.0", id: 1, method, params })
	.then(res => res.data)
	.then(data => { if (data.error) throw data.error; else return data.result as T })

const delay = (time: number) => new Promise(resolve => setTimeout(resolve, time))

;(async function()
{
	let conn = await r.connect(RETHINK_URI)
	console.log(`connected to rethink`)
	
	// init db
	let dbs = await r.dbList().run(conn)
	if (dbs.indexOf(DB_NAME) == -1)
		await r.dbCreate(DB_NAME).run(conn), console.log(`created db ${DB_NAME}`)
	else
		console.log(`db ${DB_NAME} found`)

	let db = r.db(DB_NAME)
	let init = async () =>
	{
		let tables = await db.tableList().run(conn)
		if (tables.indexOf(TABLE_BLOCKS) == -1)
			await db.tableCreate(TABLE_BLOCKS, { primaryKey: "hash" }).run(conn)
		if ((await db.table(TABLE_BLOCKS).indexList().run(conn)).indexOf('index') == -1)
		{
			await db.table(TABLE_BLOCKS).indexCreate('index').run(conn)
			await db.table(TABLE_BLOCKS).indexWait('index').run(conn)
		}
		if (tables.indexOf(TABLE_TXS) == -1)
			await db.tableCreate(TABLE_TXS, { primaryKey: "txid" }).run(conn)
	}
	await init()

	console.log(`inited table`)
	
	let getBlocks = async () =>
	{
		let blockHeight = await api<number>("getblockcount")
		console.log(`block height: ${blockHeight}`)
		let lastBlock = await r.branch(
			db.table(TABLE_BLOCKS).isEmpty(),
				-1,
				db.table(TABLE_BLOCKS)
					.max({ index: 'index' })('index')
					.default(-1)
		).run(conn)

		console.log(`last block: ${lastBlock}`)
		if (blockHeight <= lastBlock)
			return console.error("chain is unsynced"), delay(30000)

		const BATCH_SIZE = blockHeight - lastBlock
		
		for (let i = 0; i < BATCH_SIZE; i++)
		{
			let idx = lastBlock + 1 + i
			if (idx == blockHeight)
				return delay(3000)
				
			const blockHash = await api<string>("getblockhash", [idx])
			//console.log(blockHash)
			const block = await api<IBlock>("getblock", [blockHash])
			//console.log(block)
			await db.table(TABLE_BLOCKS).insert(block).run(conn)

			const txids = block['tx']
			for (let j = 0; j < txids.length; ++j) {
				try {
					const tx = await api<ITransaction>("getrawtransaction", [txids[j], true])
					//console.log(tx)
					await db.table(TABLE_TXS).insert(tx).run(conn)
				} catch (error) {
					//console.error(error)
				}
			}
		}
	}
	console.log(`fetching blocks...`)

	while(true)
	{
		if (!conn.isOpen())
		{
			console.log(`lost connection to rethink!`)
			conn = await r.connect(RETHINK_URI)
			console.log(`rethink connection restored`)
		}
		
		await getBlocks()
	}
})()
/*global console*/

import { Lyra } from '@nearform/lyra'
import fs from 'fs'
import { readFile, writeFile } from 'fs/promises'
import protobufjs from 'protobufjs'
import readline from 'readline'

const db = new Lyra({
  schema: {
    type: 'string',
    title: 'string',
    category: 'string'
  },
  edge: true
})

function populateDB() {
  console.log('Populating the database...')
  return new Promise(async resolve => {
    const fileStream = fs.createReadStream('./dataset/title5.tsv')
    const rl = readline.createInterface({
      input: fileStream,
      crlfDelay: Infinity
    })

    for await (const row of rl) {
      const [, type, title, , , , , , category] = row.split('\t')

      await db.insert({
        type,
        title,
        category
      })
    }

    console.log(db.getDocs)
    console.log(db.getIndex)
    console.time('saving index')
    const proto = await protobufjs.load('poc.proto')
    const Payload = proto.lookupType('poc.Payload')
    const serialized = Payload.encode({ docs: db.getDocs, index: db.getIndex }).finish()
    console.log(serialized.length)
    await writeFile('dataset/title.pbf', serialized)
    console.timeEnd('saving index')

    resolve(1)
  })
}

function restoreDB() {
  return new Promise(async resolve => {
    console.time('restoring index')
    const proto = await protobufjs.load('poc.proto')
    const Payload = proto.lookupType('poc.Payload')
    const { docs, index } = Payload.decode(await readFile('dataset/title.pbf'))
    console.timeEnd('restoring index')

    db.setDocs = docs
    db.setIndex = index

    resolve(1)
  })
}

async function main() {
  await populateDB()

  console.log('--------------------------------')
  console.log('Results after 1000 iterations')
  console.log('--------------------------------')

  const searchOnAllIndices = await searchBenchmark(db, {
    term: 'believe',
    properties: '*'
  })
  console.log(`Searching "believe" through 1M entries in all indices: ${searchOnAllIndices}`)

  const exactSearchOnAllIndices = await searchBenchmark(db, {
    term: 'believe',
    properties: '*',
    exact: true
  })
  console.log(`Exact search for "believe" through 1M entries in all indices: ${exactSearchOnAllIndices}`)

  const typoTolerantSearch = await searchBenchmark(db, {
    term: 'belve',
    properties: '*',
    tolerance: 2
  })
  console.log(`Typo-tolerant search for "belve" through 1M entries in all indices: ${typoTolerantSearch}`)

  const searchOnSpecificIndex = await searchBenchmark(db, {
    term: 'believe',
    properties: ['title']
  })
  console.log(`Searching "believe" through 1M entries in the "title" index: ${searchOnSpecificIndex}`)

  const searchOnSpecificIndex2 = await searchBenchmark(db, {
    term: 'criminal minds',
    properties: ['title']
  })
  console.log(`Searching "criminal minds" through 1M entries in the "title" index: ${searchOnSpecificIndex2}`)

  const searchOnSpecificIndex3 = await searchBenchmark(db, {
    term: 'musical',
    properties: ['category'],
    exact: true
  })
  console.log(`Searching "musical" through 1M entries in the "category" index: ${searchOnSpecificIndex3}`)

  const searchOnSpecificIndex4 = await searchBenchmark(db, {
    term: 'hero',
    properties: ['title']
  })
  console.log(`Searching "hero" through 1M entries in the "title" index: ${searchOnSpecificIndex4}`)
}

async function searchBenchmark(db, query) {
  const results = Array.from({ length: 1000 })

  for (let i = 0; i < results.length; i++) {
    const { elapsed } = await db.search(query)
    const isMicrosecond = elapsed.endsWith('μs')
    const timeAsStr = isMicrosecond ? elapsed.replace('ms', '') : elapsed.replace('μs', '')
    const time = parseInt(timeAsStr) * (isMicrosecond ? 1 : 1000)
    results[i] = time
  }

  const total = Math.floor(results.reduce((x, y) => x + y, 0) / results.length)

  return total > 1000 ? `${total}ms` : `${total}μs`
}

main()

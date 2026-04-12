import { describe, it, expect, vi, beforeEach } from 'vitest'
import { DynamodbBridge } from './bridge.js'

const mockSend = vi.fn()
const mockDestroy = vi.fn()

vi.mock('@aws-sdk/client-dynamodb', () => ({
  DynamoDBClient: vi.fn(() => ({ send: mockSend, destroy: mockDestroy })),
  ListTablesCommand: vi.fn(input => ({ type: 'ListTables', input })),
  DescribeTableCommand: vi.fn(input => ({ type: 'DescribeTable', input })),
}))

vi.mock('@aws-sdk/lib-dynamodb', () => ({
  DynamoDBDocumentClient: { from: vi.fn(c => ({ send: c.send })) },
  ScanCommand: vi.fn(input => ({ type: 'Scan', input })),
}))

type MockCommand = { type: string; input: Record<string, unknown> }

const defaultDescribeResponse = {
  Table: {
    KeySchema: [{ KeyType: 'HASH', AttributeName: 'id' }],
    ItemCount: 42,
  },
}

const defaultListTablesResponse = {
  TableNames: ['users', 'products'],
  LastEvaluatedTableName: undefined,
}

function makeRows(count: number): Record<string, unknown>[] {
  return Array.from({ length: count }, (_, i) => ({ id: `r${i + 1}`, name: `Row ${i + 1}` }))
}

describe('DynamodbBridge', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  describe('constructor', () => {
    it('throws if region is missing', () => {
      expect(() => new DynamodbBridge({})).toThrow('DynamodbBridge requires a "region" config string')
    })

    it('throws if region is not a string', () => {
      expect(() => new DynamodbBridge({ region: 123 })).toThrow(
        'DynamodbBridge requires a "region" config string',
      )
    })

    it('accepts valid region', () => {
      expect(() => new DynamodbBridge({ region: 'us-east-1' })).not.toThrow()
    })

    it('accepts explicit credentials', () => {
      expect(
        () =>
          new DynamodbBridge({
            region: 'us-east-1',
            accessKeyId: 'AK',
            secretAccessKey: 'SK',
            sessionToken: 'token',
          }),
      ).not.toThrow()
    })
  })

  describe('connect', () => {
    it('sends ListTablesCommand to verify connectivity', async () => {
      const bridge = new DynamodbBridge({ region: 'us-east-1' })
      mockSend.mockResolvedValueOnce(defaultListTablesResponse)
      await bridge.connect()
      expect(mockSend).toHaveBeenCalledOnce()
      const cmd = mockSend.mock.calls[0]![0] as MockCommand
      expect(cmd.type).toBe('ListTables')
    })
  })

  describe('disconnect', () => {
    it('calls destroy on the raw client', async () => {
      const bridge = new DynamodbBridge({ region: 'us-east-1' })
      mockSend.mockResolvedValueOnce(defaultListTablesResponse)
      await bridge.connect()
      await bridge.disconnect()
      expect(mockDestroy).toHaveBeenCalledOnce()
    })

    it('throws after disconnect when trying to use bridge', async () => {
      const bridge = new DynamodbBridge({ region: 'us-east-1' })
      mockSend.mockResolvedValueOnce(defaultListTablesResponse)
      await bridge.connect()
      await bridge.disconnect()
      await expect(bridge.count('users')).rejects.toThrow('DynamodbBridge is not connected')
    })
  })

  describe('listTargets', () => {
    it('returns all table names, paginating until LastEvaluatedTableName is absent', async () => {
      const bridge = new DynamodbBridge({ region: 'us-east-1' })
      mockSend.mockResolvedValueOnce(defaultListTablesResponse)
      await bridge.connect()

      mockSend
        .mockResolvedValueOnce({ TableNames: ['users', 'products'], LastEvaluatedTableName: 'products' })
        .mockResolvedValueOnce({ TableNames: ['orders'], LastEvaluatedTableName: undefined })

      const tables = await bridge.listTargets()
      expect(tables).toEqual(['users', 'products', 'orders'])
    })
  })

  describe('count', () => {
    it('sends Scan with Select COUNT and returns Count', async () => {
      const bridge = new DynamodbBridge({ region: 'us-east-1' })
      mockSend.mockResolvedValueOnce(defaultListTablesResponse)
      await bridge.connect()

      mockSend.mockResolvedValueOnce({ Count: 7 })

      const result = await bridge.count('users')
      expect(result).toBe(7)
      const cmd = mockSend.mock.calls[1]![0] as MockCommand
      expect(cmd.type).toBe('Scan')
      expect(cmd.input['Select']).toBe('COUNT')
    })

    it('returns 0 when Count is undefined', async () => {
      const bridge = new DynamodbBridge({ region: 'us-east-1' })
      mockSend.mockResolvedValueOnce(defaultListTablesResponse)
      await bridge.connect()
      mockSend.mockResolvedValueOnce({})
      expect(await bridge.count('users')).toBe(0)
    })
  })

  describe('read', () => {
    async function connectedBridge() {
      const bridge = new DynamodbBridge({ region: 'us-east-1' })
      mockSend.mockResolvedValueOnce(defaultListTablesResponse)
      await bridge.connect()
      return bridge
    }

    it('fetches PK via DescribeTable, scans rows, fetches approximate total', async () => {
      const bridge = await connectedBridge()
      const rows = makeRows(2)

      mockSend
        .mockResolvedValueOnce(defaultDescribeResponse) // getPrimaryKey
        .mockResolvedValueOnce({ Items: rows, ScannedCount: 2, LastEvaluatedKey: undefined })
        .mockResolvedValueOnce(defaultDescribeResponse) // approximate total

      const result = await bridge.read('users')
      expect(result.rows).toHaveLength(2)
      expect(result.rows[0]).toMatchObject({ id: 'r1', name: 'Row 1' })
      expect(result.total).toBe(42)
      expect(result.nextCursor).toBeUndefined()
    })

    it('caches PK — DescribeTable called once per target for getPrimaryKey', async () => {
      const bridge = await connectedBridge()
      const rows = makeRows(1)

      mockSend
        .mockResolvedValueOnce(defaultDescribeResponse) // getPrimaryKey (1st read)
        .mockResolvedValueOnce({ Items: rows, LastEvaluatedKey: undefined })
        .mockResolvedValueOnce(defaultDescribeResponse) // total (1st read)
        .mockResolvedValueOnce({ Items: rows, LastEvaluatedKey: undefined }) // (2nd read — PK cached)
        .mockResolvedValueOnce(defaultDescribeResponse) // total (2nd read)

      await bridge.read('users')
      await bridge.read('users')

      const allCmds = (mockSend.mock.calls as MockCommand[][]).map(c => c[0]!)
      const describeCalls = allCmds.filter(c => c.type === 'DescribeTable')
      // 1 getPrimaryKey + 2 totals = 3
      expect(describeCalls).toHaveLength(3)
    })

    it('sets nextCursor when hasMore and LastEvaluatedKey is present', async () => {
      const bridge = await connectedBridge()

      mockSend
        .mockResolvedValueOnce(defaultDescribeResponse)
        .mockResolvedValueOnce({
          Items: makeRows(3), // limit=2, fetched limit+1=3 → hasMore
          LastEvaluatedKey: { id: 'r2' },
          ScannedCount: 3,
        })
        .mockResolvedValueOnce(defaultDescribeResponse)

      const result = await bridge.read('users', { limit: 2 })
      expect(result.rows).toHaveLength(2)
      expect(result.nextCursor).toBe(JSON.stringify({ id: 'r2' }))
    })

    it('passes ExclusiveStartKey when cursor is set', async () => {
      const bridge = await connectedBridge()
      const cursor = JSON.stringify({ id: 'r5' })

      mockSend
        .mockResolvedValueOnce(defaultDescribeResponse)
        .mockResolvedValueOnce({ Items: makeRows(1), LastEvaluatedKey: undefined })
        .mockResolvedValueOnce(defaultDescribeResponse)

      await bridge.read('users', { cursor })

      const allCmds = (mockSend.mock.calls as MockCommand[][]).map(c => c[0]!)
      const scanCmd = allCmds.find(c => c.type === 'Scan')!
      expect(scanCmd.input['ExclusiveStartKey']).toEqual({ id: 'r5' })
    })

    it('applies ProjectionExpression for fields option', async () => {
      const bridge = await connectedBridge()

      mockSend
        .mockResolvedValueOnce(defaultDescribeResponse)
        .mockResolvedValueOnce({ Items: [{ id: 'r1', name: 'Row 1' }], LastEvaluatedKey: undefined })
        .mockResolvedValueOnce(defaultDescribeResponse)

      await bridge.read('users', { fields: ['id', 'name'] })

      const allCmds = (mockSend.mock.calls as MockCommand[][]).map(c => c[0]!)
      const scanCmd = allCmds.find(c => c.type === 'Scan')!
      expect(scanCmd.input['ProjectionExpression']).toBe('#f0, #f1')
      expect(scanCmd.input['ExpressionAttributeNames']).toEqual({ '#f0': 'id', '#f1': 'name' })
    })
  })

  describe('query', () => {
    async function connectedBridge() {
      const bridge = new DynamodbBridge({ region: 'us-east-1' })
      mockSend.mockResolvedValueOnce(defaultListTablesResponse)
      await bridge.connect()
      return bridge
    }

    it('sends Scan with no FilterExpression when where is empty', async () => {
      const bridge = await connectedBridge()
      mockSend.mockResolvedValueOnce({ Items: makeRows(2), ScannedCount: 2 })
      const result = await bridge.query('users', {})
      expect(result.rows).toHaveLength(2)
      const scanCmd = mockSend.mock.calls[1]![0] as MockCommand
      expect(scanCmd.type).toBe('Scan')
      expect(scanCmd.input['FilterExpression']).toBeUndefined()
    })

    it('builds FilterExpression for $eq', async () => {
      const bridge = await connectedBridge()
      mockSend.mockResolvedValueOnce({ Items: [{ id: 'r1', status: 'active' }], ScannedCount: 5 })
      await bridge.query('users', { where: { status: { $eq: 'active' } } })
      const scanCmd = mockSend.mock.calls[1]![0] as MockCommand
      expect(scanCmd.input['FilterExpression']).toBe('#n0 = :v0')
      expect(scanCmd.input['ExpressionAttributeNames']).toEqual({ '#n0': 'status' })
      expect(scanCmd.input['ExpressionAttributeValues']).toEqual({ ':v0': 'active' })
    })

    it('builds FilterExpression for $gt', async () => {
      const bridge = await connectedBridge()
      mockSend.mockResolvedValueOnce({ Items: [], ScannedCount: 0 })
      await bridge.query('users', { where: { age: { $gt: 18 } } })
      const scanCmd = mockSend.mock.calls[1]![0] as MockCommand
      expect(scanCmd.input['FilterExpression']).toBe('#n0 > :v0')
    })

    it('builds FilterExpression for $in with OR clauses', async () => {
      const bridge = await connectedBridge()
      mockSend.mockResolvedValueOnce({ Items: makeRows(2), ScannedCount: 10 })
      await bridge.query('users', { where: { status: { $in: ['active', 'pending'] } } })
      const scanCmd = mockSend.mock.calls[1]![0] as MockCommand
      expect(String(scanCmd.input['FilterExpression'])).toContain('OR')
    })

    it('throws on unknown operator', async () => {
      const bridge = await connectedBridge()
      await expect(
        bridge.query('users', { where: { age: { $unknown: 42 } } }),
      ).rejects.toThrow('Unknown operator "$unknown"')
    })

    it('applies limit and offset on result rows', async () => {
      const bridge = await connectedBridge()
      mockSend.mockResolvedValueOnce({ Items: makeRows(10), ScannedCount: 10 })
      const result = await bridge.query('users', { limit: 3, offset: 2 })
      expect(result.rows).toHaveLength(3)
      expect(result.rows[0]).toMatchObject({ id: 'r3' })
    })

    it('handles plain equality (non-operator) value', async () => {
      const bridge = await connectedBridge()
      mockSend.mockResolvedValueOnce({ Items: [], ScannedCount: 0 })
      await bridge.query('users', { where: { active: true } })
      const scanCmd = mockSend.mock.calls[1]![0] as MockCommand
      expect(scanCmd.input['FilterExpression']).toBe('#n0 = :v0')
      expect(scanCmd.input['ExpressionAttributeValues']).toEqual({ ':v0': true })
    })
  })
})

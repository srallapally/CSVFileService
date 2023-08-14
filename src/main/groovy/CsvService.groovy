@Grab(group='org.apache.commons', module='commons-csv', version='1.8')
import org.apache.commons.csv.CSVFormat
import org.apache.commons.csv.CSVParser
import org.apache.commons.csv.CSVPrinter

/**
 * A service for reading and writing CSV files.
 */
@Singleton
class CsvService {
    def delimiter = ','
    def enclosure = '"'
    def newline = '\n'
    def keyFields = []

    def configure(delimiter = ',', enclosure = '"', newline = '\n', keyFields = []) {
        this.delimiter = delimiter
        this.enclosure = enclosure
        this.newline = newline
        this.keyFields = keyFields
    }

    def readCsv(file) {
        def parser = CSVParser.parse(file, charset, CSVFormat.DEFAULT.withDelimiter(delimiter).withQuote(enclosure).withRecordSeparator(newline).withFirstRecordAsHeader())
        def schema = parser.headerMap.keySet().toList()
        def records = parser.records.collect { row -> schema.collectEntries { key -> [(key): row[key]] } }
        return [schema: schema, records: records]
    }

    def writeCsv(file, records, schema) {
        def writer = new FileWriter(file, false)
        def csvWriter = new CSVPrinter(writer, CSVFormat.DEFAULT.withDelimiter(delimiter).withQuote(enclosure).withRecordSeparator(newline).withHeader(schema as String[]))
        records.each { record -> csvWriter.printRecord(record.values()) }
        writer.close()
    }

    def sortRecordsByKeys(records) {
        return records.sort { a, b ->
            keyFields.each { key ->
                def comparison = a[key].compareTo(b[key])
                if (comparison != 0) return comparison
            }
            return 0
        }
    }

    def logOldRecord(file, record) {
        def writer = new File(file).newWriter(true) // Open file in append mode
        def csvWriter = new CSVPrinter(writer, delimiter: delimiter, enclosure: enclosure, newline: newline)
        record['timestamp'] = new Date().format('yyyy-MM-dd HH:mm:ss')
        csvWriter.writeLine(record.values())
        writer.close()
    }

    def updateRecord(file, keys, values, newRecord, logFile) {
        def data = readCsv(file)
        def updatedRecords = data.records.collect {
            keys.eachWithIndex { key, idx ->
                if (it[key] == values[idx]) {
                    logOldRecord(logFile, it)
                    return newRecord.collectEntries()
                }
            }
            return it
        }
        def sortedRecords = sortRecordsByKeys(updatedRecords)
        writeCsv(file, sortedRecords, data.schema)
    }

    def deleteRecord(file, keys, values, logFile) {
        def data = readCsv(file)
        def remainingRecords = data.records.findAll {
            keys.eachWithIndex { key, idx ->
                if (it[key] == values[idx]) {
                    logOldRecord(logFile, it)
                    return false
                }
            }
            return true
        }
        def sortedRecords = sortRecordsByKeys(remainingRecords)
        writeCsv(file, sortedRecords, data.schema)
    }
    def processCsvWithEntitlements(file, doesFileContainEntitlements, fileHasMultipleRowsPerUser,
                                   hasEntitlementDescription, entitlementsMap) {
        def data = readCsv(file)
        def uniqueEntitlements = [:]
        def groupedRecords = data.records.groupBy { record ->
            keyFields.collect { key -> record[key] }.join(',')
        }.collect { key, records ->
            def record = records.first().clone()
            if (doesFileContainEntitlements) {
                if (fileHasMultipleRowsPerUser && hasEntitlementDescription) {
                    entitlementsMap.each { entitlement, columns ->
                        columns.each { column ->
                            uniqueEntitlements[record[column]] = record[entitlement]
                            record[column] = null // Discard the entitlement description column
                        }
                    }
                }
                record['entitlements'] = records.collect { rec ->
                    entitlementsMap.values().flatten().collect { col -> rec[col] }.join(',')
                }.join(',')
            }
            return record
        }

        // Write unique entitlement ids and descriptions to a new file
        if (uniqueEntitlements) {
            uniqueEntitlements.each { entitlement, description ->
                def entitlementFile = new File(entitlement + '.csv')
                def writer = entitlementFile.newWriter(true)
                writer.writeLine("$entitlement,$description")
                writer.close()
            }
        }

        // Write grouped records to the original file
        writeCsv(file, groupedRecords, data.schema + (doesFileContainEntitlements ? ['entitlements'] : []))
    }
}

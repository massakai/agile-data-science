from avro import schema, datafile, io
import pprint

OUTFILE_NAME = "/tmp/messages.avro"

record_reader = io.DatumReader()
df_reader = datafile.DataFileReader(
    open(OUTFILE_NAME),
    record_reader
)

pp = pprint.PrettyPrinter()
for record in df_reader:
    pp.pprint(record)

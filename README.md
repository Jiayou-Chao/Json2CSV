# Json2CSV

## Introduction

My boss keeps asking to convert JSON files into CSV files, so I wrote this module, which can automatically convert a folder of JSON files into CSV format.

## How to use it

To use it, simply run the following command:

```bash
$ python parse_json.py -f <input_folder> -i id1 id2 id3
```

The `-f` flag is used to specify the input folder, and the `-i` flag is used to specify the IDs used the JSON files to be converted. `-i` is optional, if not specified, no ID columns will be used.

# Disclaimer

I tried my best to make this module reliable, but I am not responsible for the results. Please be aware that this module is not widely tested, so use it at your own risk. If you find any bugs, please report them to me.
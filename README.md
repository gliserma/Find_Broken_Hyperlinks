# Broken Links Webcrawler
Developed to identify broken links on website for
the Gilder Lehrman Insitute of American History.

## Implementation.
- A simple python script that can be launched from the terminal.

## Optional Command Line Arguments:
- --fname: desired name for the output file
- --number: how many pages should be searched

## Output: Two Files
- fname: All pages visited and all the links contained in those pages as a csv
- broken_fname: All broken links, i.e. origin page, destination page, anchor text

## Requirements
- Python3.6+
- Scrapy 2.5.0

## Future Steps
- find broken images
- find pages with code fragments showing as text
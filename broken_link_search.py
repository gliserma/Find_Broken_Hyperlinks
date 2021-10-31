import argparse
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
from scrapy.crawler import CrawlerProcess
import csv

# Dictionary containing http status codes
# with their corresponding meanings.
RESPONSE_CODES = {
    200: "OK",
    301: "Moved Permanently",
    302: "Found",
    400: "Bad Request",
    401: "Unauthorized",
    403: "Forbidden",
    404: "Not Found"
}
# Names of the CSV fields
FIELDS = [
    'origin_url',           # 0
    'origin_status_code',   # 1
    'status_description',   # 2
    'outbound_anchor_text', # 3
    'outbound_hyperlink',   # 4
    'outbound_status_code', # 5
]

class GLI_Spider(CrawlSpider):
    """
    Searches the Gilder Lehrman website 
    for all hyperlinks and page statuses.
    """
    # Parameters for this Crawl Spider
    name = 'broken_links_from_homepage'
    allowed_domains = ['gilderlehrman.org']
    start_urls = [
        'https://www.gilderlehrman.org/', 
        'https://www.gilderlehrman.org/news/'
    ]
    handle_httpstatus_list = [
        200, 
        301, 
        302, 
        400, 
        401, 
        403, 
        404
    ]
    # How and where the spider will crawl
    rules = [ 
        Rule(LinkExtractor(allow_domains='gilderlehrman.org'), 
        callback='parse_info', 
        follow=True) 
    ]

    # Another link extractor -- not for crawling but 
    # for finding all hyperlinks on a given page.
    le = LinkExtractor(
        allow_domains='gilderlehrman.org', 
        unique=False
    )

    def parse_info(self, response):
        """
        Defines what the spider will extract 
        from a page that it visits.
        """
        # Extract information from the reponse
        status = response.status
        desc = RESPONSE_CODES[status]
        internal_links = self.le.extract_links(response)

        # OUTPUT for the CSV
        # If the page is not working:
        if status >= 400:
            yield {
                # Collect only the url and status for this page
                    FIELDS[0] : response.url,
                    FIELDS[1] : status,
                    FIELDS[2] : desc,
            }
        # If the page is working:
        else: 
            for out_link in internal_links:
                # Also collect all outbound links on this page
                yield {
                    FIELDS[0] : response.url,
                    FIELDS[1] : status,
                    FIELDS[2] : desc,
                    FIELDS[3] : format_for_csv(out_link.text.strip()),
                    FIELDS[4] : remove_bookmarks(out_link.url)
                }

def remove_bookmarks(hyperlink):
    """
    Strips any text following a hashtag ('#') in a hyperlink.
    """
    if '#' in hyperlink:
        index = hyperlink.find('#')
        hyperlink = hyperlink[0:index]
    return hyperlink

def format_for_csv(description):
    """
    Removes commas and newlines from text that should
    be a standalone cell in the output csv.
    """
    split_desc = str(description).split(sep=',')
    split_desc = " ".join(split_desc).splitlines()
    return (" ".join(split_desc))

class CSV_URLs():
    """
    Processes the raw results of the webcrawl and outputs
    a new csv file of the broken links.

    One limitation of Scrapy is that it is not always
    able to report the origin page by which it arrived
    at a broken page. This class resolves this issue by
    essentially reversing the direction of the directed
    graph so that the destination pages now point to their
    origins. The ultimate output of this process is a new
    csv file exclusively containing the broken links.
    """
    def __init__(self, fname):
        self.filename = fname
        self.broken_pages = dict()
        self.find_broken_pages()
        self.broken_links = list()
        self.find_broken_links()
        self.rewrite_csv()

    def find_broken_pages(self):
        """
        Scans the csv file for any pages with a status code
        of 400 or greater.
        """
        # Open the CSV file and prepare it for reading
        with open(self.filename, newline='') as link_list:
            link_reader = csv.DictReader(link_list, delimiter=',')

            # Find all broken pages in the csv
            for row in link_reader:
                status = int(row[FIELDS[1]])
                if status == 400 or status == 401 or status == 404:
                    self.broken_pages[row[FIELDS[0]]] = status

    def find_broken_links(self):
        """
        Scans the csv file for any outbound links that
        lead to a broken page.
        """
        with open(self.filename, newline='') as link_list:
            link_reader = csv.DictReader(link_list, delimiter=',')

            # Find all broken links in the csv
            for row in link_reader:
                if row[FIELDS[4]] in self.broken_pages:
                    self.broken_links.append({
                        FIELDS[0]: row[FIELDS[0]],
                        FIELDS[1]: row[FIELDS[1]],
                        FIELDS[2]: row[FIELDS[2]],
                        FIELDS[3]: row[FIELDS[3]],
                        FIELDS[4]: row[FIELDS[4]],
                        FIELDS[5]: self.broken_pages[row[FIELDS[4]]],
                    })

    def rewrite_csv(self):
        """
        Only pages with broken links persist in the 
        final version of the output csv
        """
        with open("broken_" + self.filename, 'w', newline='') as broken_link_list:
            bll_writer = csv.DictWriter(broken_link_list, FIELDS)
            bll_writer.writeheader()

            for entry in self.broken_links:
                bll_writer.writerow({
                        FIELDS[0]: entry[FIELDS[0]],
                        FIELDS[1]: entry[FIELDS[1]],
                        FIELDS[2]: entry[FIELDS[2]],
                        FIELDS[3]: entry[FIELDS[3]],
                        FIELDS[4]: entry[FIELDS[4]],
                        FIELDS[5]: entry[FIELDS[5]],
                })
    

def main():
    # GET ARGUMENTS
    parser = argparse.ArgumentParser(description='options')
    # Parameters: filename and number of pages to search
    parser.add_argument('--fname', dest='fname', help='Name of output file')
    parser.add_argument('--number', dest='number', help='Number of pages to search')
    args = parser.parse_args()
    # Parse the arguments
    if args.fname is not None:
        links_fname = args.fname + ".csv"
    else:
        links_fname = 'gli_hyperlinks.csv'

    if args.number is not None and args.number.isdigit():
        num_searches = int(args.number)
    else:
        num_searches = 10000

    # Initialize settings for webcrawl
    process = CrawlerProcess(settings={
        'FEEDS': {
            links_fname : {
                'format': 'csv',
                'fields' : FIELDS,
                'overwrite': True,
            },
        },
        'CLOSESPIDER_PAGECOUNT': num_searches,
        'LOG_LEVEL': 'CRITICAL',
        'DEPTH_PRIORITY': 1,
        'SCHEDULER_DISK_QUEUE': 'scrapy.squeues.PickleFifoDiskQueue',
        'SCHEDULER_MEMORY_QUEUE': 'scrapy.squeues.FifoMemoryQueue',
    })

    # Start the webcrawl
    process.crawl(GLI_Spider)
    process.start()

    # For all broken pages, find the source hyperlink
    adjust_csv = CSV_URLs(links_fname)

if __name__ == "__main__":
    main()
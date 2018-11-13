LOGIN = ''
PASSWORD = ""

## Database
DB_USERNAME = ''
DB_PASS = ''
DB_HOST = ''
DB = ''


## Downloading User Meta
MAX_DAYS = 30  # maximum age of posts to see who like/comment on a target previously
SPLIT_USERS = True

## Stats related
MIN_FOLLOWER = 1000
TOGGLE_VPN = 45 # in minutes
IMAGE_PER_USER = 2 # number of images to send to Vision API
MAX_COST = 100

## Approved IP for google sql
IP = [
    "196.52.34.1","196.52.34.2","196.52.34.3","196.52.34.4","196.52.34.5",
    "196.52.34.6","196.52.34.7","196.52.34.8","196.52.34.9","196.52.34.10",
    "196.52.34.11","196.52.34.12","196.52.34.13","196.52.34.14","196.52.34.15",
    "196.52.34.16","196.52.34.17","196.52.34.18","196.52.34.19","196.52.34.20",
    "196.52.34.21","196.52.34.22","196.52.34.23","196.52.34.24","196.52.34.25",
    "196.52.34.26","206.189.156.136"
]

## Slack
SUMMARY_WEBHOOK = ''
TIMING_WEBHOOK = ''
CRITICAL_WEBHOOK = ''

# Concurrency
MAX_WORKERS = 3

## API file
from os.path import dirname, abspath, join
API_ACC_KEY = join(dirname((abspath(__file__))),"Socialcracy-995f76f09afd.json")

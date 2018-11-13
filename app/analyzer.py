import db
from scraper import get_logger
import argparse
import socket
import os
from constants import API_ACC_KEY
import io
import os
from google.cloud import vision
from google.cloud.vision import types
import requests
from constants import MIN_FOLLOWER,IMAGE_PER_USER, MAX_COST

class Analyzer:
    def __init__(self,**kwargs):
        s_logger.info(hostname + ': Analyzer initialized') ## logging
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = API_ACC_KEY ## Env variable
        self.db = db.DataBase(c_logger, s_logger, t_logger)

        ## attributes
        self.image_per_user = kwargs['image_per_user']
        self.min_follower = kwargs['min_follower']
        self.max_cost = kwargs['max_cost']
    
    def analyze(self):
        """
        Pull post images link from DB and feed to Google's Image API

        Args:
        Returns:
        """

        # Get image link
        urls = self.db.get_image(self.image_per_user,self.min_follower)
        
        # Instantiates a client
        client = vision.ImageAnnotatorClient()
        c = 1

        for item, row in urls.iterrows():
            if (c/1000*1.5) > MAX_COST:
                c_logger('Max cost reached. Terminated')
                exit()

            # Download the image
            url = row['post_url']
            r = requests.get(url, allow_redirects=True)
            open('download.jpg', 'wb').write(r.content)

            # The name of the image file to annotate
            file_name = os.path.join( os.path.dirname(__file__),
                        'download.jpg')

            # Load the image into memory
            with io.open(file_name, 'rb') as image_file:
                content = image_file.read()

            image = types.Image(content=content)

            # Performs label detection on the image file
            response = client.label_detection(image=image)
            labels = response.label_annotations
            des = ','.join(str(label.description) for label in labels)
            score = ','.join(str(label.score) for label in labels)
            topicality = ','.join(str(label.topicality) for label in labels)

            # Delete file
            os.remove("download.jpg")

            # Save result back in DB
            self.db.add_image_label(row['postID'],des,score,topicality)

            c += 1
        
def main():
    """
    Command line interaction
    """

    # creater parser
    parser = argparse.ArgumentParser(description = 'Image analyzer for Instagram Scraper')
    parser.add_argument('--analyze', action = 'store_true', help = 'Submit photos in Post DB to Google Vision API for analyzing')
    parser.add_argument('--image_per_user',default = IMAGE_PER_USER, help = 'Number of image to analyzer for each user')
    parser.add_argument('--min_follower',default = MIN_FOLLOWER, help = 'Minimum followers to analyze')
    parser.add_argument('--max_cost',default = MAX_COST, help = 'Maximum cost to incur per run(in USD)')
    args = parser.parse_args()
    
    # create Scraper object
    app = Analyzer(**vars(args))

    # parse input and run accordingly
    app.analyze() if args.analyze else None

if __name__ == "__main__":
    c_logger, s_logger, t_logger = get_logger()
    hostname = str(socket.gethostname())
    database = db.DataBase(c_logger, s_logger, t_logger)
    main()
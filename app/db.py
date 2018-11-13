import pymysql
import time
from constants import DB_HOST,DB_USERNAME,DB_PASS,DB, SUMMARY_WEBHOOK, TIMING_WEBHOOK, CRITICAL_WEBHOOK
from datetime import datetime
import socket
import re
from scraper import get_logger
from pandas import DataFrame, read_sql
from random import shuffle

hostname = str(socket.gethostname())
class DataBase:
    def __init__(self, c_logger, s_logger, t_logger,split):
        self.c_logger, self.s_logger, self.t_logger = c_logger, s_logger, t_logger
        self.today = datetime.now().strftime('%d-%m-%Y')
        self.split = split

    def connect(self):
        """
        Connect to mysql and set self.cur
        """
        attempt = 0

        while attempt < 5:
            try:
                self.db = pymysql.connect(
                    host = DB_HOST,
                    user = DB_USERNAME,
                    password = DB_PASS,
                    db = DB
                )
                self.cur = self.db.cursor()
                return("")
            except pymysql.err.OperationalError:
                attempt += 1
                time.sleep(10)

    def get_queue_cutters(self):
        """
        Get usernames in Cut DB to supercede profiles queued for scraping

        Returns:
            profiles(str): first username in Cut DB
        """
        # Connec to db
        self.connect()

        # Get usernames from Cut DB
        self.cur.execute("SELECT username from Cut")
        cut = self.cur.fetchall()
        cut = [item[0] for item in cut]

        if cut: # test if there is any profiles in cut DB
            cut = cut[0]
            res = self.in_profile(cut)
            self.cur.execute("Delete from Cut where username = '{}';".format(cut))
            self.cur.connection.commit()
            self.cur.connection.close()
            return "" if res else cut
        else: # return empty string if nothing
            return("")

    def in_profile(self,username):
        """
        Test if username in profile DB
        
        Args:
            username (str): username to check
        Returns:
            res (boolean): Yes/No if username is in DB
        """
        self.connect()
        self.cur.execute("Select * from Profile where username = '{}'".format(username))
        result = self.cur.fetchall()
        self.cur.connection.close()
        return True if result else False

    def get_incomplete(self):
        """
        Get incomplete profiles in Incomplete DB
        
        Returns:
            profiles(list): list of usernames
        """
        self.connect()
        # Clean up incomplete by checking against Profile
        self.cur.execute("SELECT distinct username from Incomplete")
        incomplete = self.cur.fetchall()
        incomplete = [item[0] for item in incomplete]

        self.cur.execute("SELECT distinct username FROM Profile")
        completed = self.cur.fetchall()
        completed = [item[0] for item in completed]

        # find unique
        incomplete = list(set(incomplete) - set(completed))

        # Update incomplete
        self.cur.execute("DELETE from Incomplete")
        self.cur.executemany("INSERT INTO Incomplete (username) values (%s)",incomplete)
        self.cur.connection.commit()

        # Split incomplete according to first letter of username
        split_username = self.split_users(incomplete)

        # fetch list
        self.cur.connection.close()
        return(split_username)
    
    def split_users(self, users):
        """
        Split users to different Digital Ocean instances
        
        Args:
            users (list): list of users to split
        
        Returns:
            split_username (list): list of usernames according to DO instances name
        """
        # Split incomplete according to first letter of username
        hostname = socket.gethostname()
        if self.split == False:
            split_username = users
        elif hostname == 'instagram-scraper': # a to m
            split_username = [i for i in users if re.match('^[a-mA-M]',i)]
        elif hostname == 'scraper2':
            split_username = [i for i in users if re.match('^(?![a-mA-M])',i)]
        else:
            self.c_logger.exception(hostname + ':Unable to split username for host {}'.format(hostname))
            split_username = users

        shuffle(split_username)
        return(split_username)

    def get_influencers(self,min_followers):
        """
        Get username of usernames who are above MIN_FOLLOWERS

        Args:
            min_followers (integers): min followers that the users should have
        
        Returns:
            influencers (list): list of usernames
        """
        self.connect()
        self.cur.execute("SELECT distinct username FROM Profile WHERE follower_count > {};".format(min_followers))
        influencers = self.cur.fetchall()
        influencers = [item[0] for item in influencers]
        influencers = self.split_users(influencers)
        self.cur.connection.close()
        return(influencers)

    def get_date(self):
        """
        Get date of last post in Post DB

        Args:
            None
        Returns:
            result(dict): username as key and date_updated as value
        """
        self.connect()
        self.cur.execute("SELECT username, MAX(date_updated) from Post GROUP BY username;")
        result = self.cur.fetchall()
        result = {item[0]:item[1] for item in result}
        self.cur.connection.close()
        return(result)

    def flatten_meta(self, meta):
        """
        Flatten meta information to lists for importing into DB

        Args:
            meta (dict): Dictionary of meta information. Usually post or users
        
        Return:
            keyStr (str): Concatenated keys of meta
            valueStr (str): Concatenated values of meta
        """
        keyStr = []
        valueStr = []
        for key,value in meta.items():
            keyStr.append(key)
            valueStr.append(self.cur.connection.escape(value))
            
        keyStr = ','.join(keyStr)
        valueStr = ','.join(valueStr)

        return(keyStr, valueStr)

    def add_Profile(self,userMeta):
        """
        Add new user information in Profile DB
        Args:
            userMeta (dict): Meta information of a user
        Returns:
            none
        """
        ## Flattening usermeta
        keyStr, valueStr = self.flatten_meta(userMeta)

        ## Updating DB
        self.connect()
        try:
            self.cur.execute('INSERT INTO Profile ({}) VALUES ({})'.format(keyStr,valueStr))
            self.cur.connection.commit()
        except Exception:
            self.c_logger.exception(hostname + ": Error adding profile into Profile db")
        self.cur.connection.close()

    
    def update_Profile(self,userMeta):
        """
        Update user information in Profile DB
        Args:
            userMeta (dict): Meta information of a user
        Returns:
            none
        """
        self.connect()
        ## Concatenate them
        val_list = []
        for key, value in userMeta.items():
            placeholder = "{} = {}".format(key,self.cur.connection.escape(value))
            val_list.append(placeholder)
        val_list = ','.join(val_list)

        ## Updating DB
        try:
            self.cur.execute('UPDATE Profile SET {} WHERE userID = {};'.format(val_list,userMeta['userID']))
            self.cur.connection.commit()
        except Exception:
            self.c_logger.exception(hostname + ": Error updating profile in Profile db")
        self.cur.connection.close()

    def add_Post(self,postMeta):
        """
        Update post information in Post DB
        
        Args:
            postMeta (dict): Post meta information 
        
        Returns:
            none
        """
        ## Flattening usermeta
        keyStr, valueStr = self.flatten_meta(postMeta)

        ## Updating DB
        self.connect()
        try:
            self.cur.execute('INSERT INTO Post ({}) VALUES ({})'.format(keyStr,valueStr))
            self.cur.connection.commit()
        except pymysql.err.IntegrityError:
            self.c_logger.exception(hostname + ": Post {} for {} not added because it is already in Post DB".format(postMeta['postID'],postMeta['username']))
        except Exception:
            self.c_logger.exception(hostname + ": Error adding post in Post DB")
        self.cur.connection.close()
        
    def update_Incomplete(self, users):
        """
        Update users in Incomplete DB
        
        Args:
            users (list): list of users to add in Incomplete
        Return:
        """

        # Insert new users
        self.connect()
        self.cur.executemany("INSERT INTO Incomplete (username) values (%s)",users)
        self.cur.connection.commit()

        # delete duplicate
        self.get_incomplete()
        

    def add_image_label(self,postID,description,score, topicality):
        """ 
        Add labels obtained from Google Image API to DB 

        Args:
            postID (str): postID to update
            label  (str): labels from google image api
        """
        self.connect()
        self.cur.execute('UPDATE Post SET label = "{}", score = "{}", topicality = "{}" WHERE postID = {};'.format(description,score,topicality, postID))
        self.cur.connection.commit()
        self.cur.connection.close()

    def get_image(self,n,min_followers):
        """
        Randomly pick n images for each user who have followers more than z
        
        Args:
            n (int): Number of images to pick for each user
            min_followers (int): Number of followers each user should have
        
        Rtns:
            sample:
        """
        # get all posts
        self.connect()
        query = ("SELECT postID, Post.label, Post.userID, Post.username, Post.post_url, Profile.follower_count"
                " FROM Post"
                " INNER JOIN Profile on Post.username = Profile.username"
                " WHERE Profile.follower_count > {} AND Post.label IS NULL;"
                )

        # Conver to df
        df = read_sql(query.format(min_followers), self.db)

        # Randomly pick them
        sample = df.groupby('username').apply(lambda x: x.sample(n, replace = True)).reset_index(drop=True)
        sample = sample.drop_duplicates(['postID'], keep = "last")
        self.cur.connection.close()
        return(sample)
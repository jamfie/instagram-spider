import instaloader
import db
import argparse
import logging
import time
import sys
import os
import requests
import socket
from webhook_logger.slack import SlackHandler, SlackFormatter
from constants import MAX_DAYS, MIN_FOLLOWER, TOGGLE_VPN, IP, SUMMARY_WEBHOOK, TIMING_WEBHOOK, CRITICAL_WEBHOOK, SPLIT_USERS
from datetime import datetime, timedelta
from itertools import takewhile, islice


class Scraper:
    def __init__(self,**kwargs):
        ## logging
        s_logger.info(hostname + ': Scraper initialized')
        
        ## attributes
        self.max_days = kwargs['max_days']
        self.min_follower = kwargs['min_follower']
        split = kwargs['split_users']

        ## instances
        self.L = instaloader.Instaloader()
        self.db = db.DataBase(c_logger, s_logger, t_logger, split)
        
    def crawl_post(self):
        """
        Download new user meta and crawl to other users who have been tagged
        in the new user's post

        Args:
            usernames (list): list of usernames to download media for

        Returns:

        Raises:
        """
        self.toggle_openvpn()
        s_logger.info('Crawling post')
        targets = self.db.get_incomplete()
        last_ip_change = time.time()
        c = 0

        for target in targets:
            # Jump queue: Supercede this current user 
            cut = self.db.get_queue_cutters()
            target = cut if cut else target
            if not target:
                continue

            # Test if current target in DB. Skip if yes
            in_DB = self.db.in_profile(target)
            if in_DB: 
                continue

            # log
            s_logger.info(hostname + ': Crawling {}/{} user: {}'.format(c,len(targets),target))
            c += 1


            # getting meta data
            start = time.time()
            try:
                self.profile = instaloader.Profile.from_username(self.L.context,target)
            except instaloader.exceptions.ProfileNotExistsException:
                c_logger.error("ProfileNotExistException: " + hostname + " :User {} not found".format(target))
                continue
            except instaloader.exceptions.ConnectionException:
                c_logger.error("ProfileNotExistException: " + hostname + " :User {} not found".format(target))
                continue
            diff = (time.time() - start) % 60
            t_logger.info(hostname + ':Took {} seconds to get userMeta for {}'.format(round(diff,2),target))
            userMeta = self.download_userMeta(self.profile)
            if userMeta:
                self.db.add_Profile(userMeta)
            else:
                continue
           
            # Get likes and comments
            if (int(userMeta['follower_count']) > self.min_follower) and (userMeta['private'] != 1):
                self.download_postMeta(self.profile,self.max_days)

            # openvpn
            if (time.time() - last_ip_change)/60 > TOGGLE_VPN:
                self.toggle_openvpn()
                last_ip_change = time.time()
    
    def download_userMeta(self,profile):
        """
        Download user meta (i.e basic information about user)

        Args:
            profile (object): Target user to get meta information for
            days (int): Max age of posts to analyze which users liked the posts before
        Returns:
            userMeta (dictionary): A dictionary of user's meta information such as bio, name etc
        Raises:
        """
        
        # Return meta
        start = time.time()
        try:
            userMeta = {
                'verified' : str(int(profile.is_verified)),
                'bio': profile.biography,
                'extLink': profile.external_url,
                'private': str(int(profile.is_private)),
                'userID': str(int(profile.userid)),
                'name': profile.full_name,
                'media': str(profile.mediacount),
                'followee_count': str(profile.followees),
                'follower_count': str(profile.followers),
                'username': profile.username,
                'profile_pic_url': profile.profile_pic_url,
                'date_updated': datetime.now().strftime('%Y-%m-%d'),
            }
        except Exception:
            c_logger.exception(hostname + ": Error downloading userMeta")
            return("")
        diff = round(time.time() - start)
        t_logger.info(hostname + ':Took {} seconds to download userMeta for {}'.format(diff,userMeta['username']))
        return(userMeta)

    def download_postMeta(self,profile,days = MAX_DAYS):
        """
        Download post meta (ie information about the post of an user)

        Args:
            profile(obj): Target user to analyze
            days(int): Max age of post which should be downloaded
        Returns:
            postMeta(dict): Post meta information
        """
        start = time.time()
        tagged_users = ""
        p = 0
        # Download post meta
        for post in takewhile(lambda p: (datetime.now() - p.date).days <= days , profile.get_posts()):
            try:
                p += 1
                post_download = time.time()
                postMeta = {
                    'postID': post.mediaid,
                    'userID': post.owner_id,
                    'username': post.owner_username,
                    'caption': post.caption,
                    'date_created': post.date.strftime('%Y-%m-%d %H:%M:%S'),
                    'likes': post.likes,
                    'date_updated' : datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'hashtag': ','.join(post.caption_hashtags) if post.caption_hashtags else "",
                    'mention': ','.join(post.caption_mentions) if post.caption_mentions else "",
                    'tagged': ','.join(post.tagged_users) if post.tagged_users else "",
                    'type': post.typename,
                    'comments': post.comments,
                    'location': post.location[2] if post.location else "",
                    'post_url': post.url
                }

                # Concatenate tagged users to str
                if postMeta['tagged']:
                    tagged_users = tagged_users + ',' + postMeta['tagged']
            except Exception:
                c_logger.exception("Error downloading userMeta")
                p -= 1
                continue

            # Update post meta in Post DB
            self.db.add_Post(postMeta)
            diff = (time.time()- post_download) % 60
            t_logger.info(hostname + ":Took {} seconds to download post {} for {}".format(round(diff), postMeta['postID'],postMeta['username']))


        # Update tagged user in Incomplete DB
        tagged_users = list(set(tagged_users.split(sep=',')))
        tagged_users = [x for x in tagged_users if x != '']
        self.db.update_Incomplete(tagged_users)
        
        # Logging
        diff = round((time.time() - start) % 60)
        s_logger.info(hostname + ': Took {} seconds to download {} posts for {}'.format(diff,p,profile.username))

    def update(self):
        """
        Update userMeta and postMeta for influencers who are above MIN_FOLLOWERS 

        Args:

        Returns:

        Raises:
        """

        # ip change
        self.toggle_openvpn()
        last_ip_change = time.time()
        s_logger.info('Updating userMeta and postMeta')
        c = 1

        # get users who are above min_followers
        influencers = self.db.get_influencers(self.min_follower)
        date_dict = self.db.get_date()

        # loop through all influencers
        for influencer in influencers:
            # log
            s_logger.info(hostname + ': Updating {}/{} user: {}'.format(c,len(influencers),influencer))
            c += 1

            # getting user meta data
            try:
                self.profile = instaloader.Profile.from_username(self.L.context,influencer)
            except instaloader.exceptions.ProfileNotExistsException:
                c_logger.error("ProfileNotExistException: " + hostname + " :User {} not found".format(influencer))
                continue
            except instaloader.exceptions.ConnectionException:
                c_logger.error("ProfileNotExistException: " + hostname + " :User {} not found".format(influencer))
                continue
            userMeta = self.download_userMeta(self.profile)
            self.db.update_Profile(userMeta)
           
            # get latest date
            try:
                latest = date_dict[userMeta['username']]
                t_logger.info('Post DB has posts up to {} for User {}'.format(latest, userMeta['username']))
            except KeyError:
                c_logger.exception('Unable to find user {} in Post DB'.format(userMeta['username']))
                continue
            days_to_update = datetime.now() - latest
            days_to_update = days_to_update.days

            # Get likes and comments
            if (int(userMeta['follower_count']) > self.min_follower) and days_to_update >0 :
                self.download_postMeta(self.profile,days_to_update)
            
            # openvpn
            if (time.time() - last_ip_change)/60 > TOGGLE_VPN:
                self.toggle_openvpn()
                last_ip_change = time.time()

    def toggle_openvpn(self):
        """
        restart/start openvpn
        """

        # find out which system
        if sys.platform == 'win32':
            OS = 'Win'
        elif sys.platform == 'darwin':
            OS = 'Mac'
        elif sys.platform == 'linux':
            OS = 'Lin'

        start = time.time()
        ip = requests.get("http://ipinfo.io/ip").text
        while ip not in IP:
            if OS == "Win":
                os.system("taskkill.exe /F /IM openvpn.exe")
                os.system('START /B openvpn --config "openvpn\\win.ovpn" > openvpn\\cmdline.txt')
            elif OS == "Mac" or OS == "Lin":
                # need to amend user permission first. Run the following in terminal
                # sudo visudo
                # then add this command to exclude the following user run ALL command without a password for a user named tom
                # tom ALL=(ALL) NOPASSWD:ALL
                fileName = 'linux.ovpn' if OS == "Lin" else 'mac.ovpn'
                os.system("sudo killall openvpn")
                os.system("sudo openvpn --config openvpn/{} --daemon".format(fileName))
            time.sleep(10)

            # log address
            ip = requests.get("http://ipinfo.io/ip").text
            ip = ip.strip()
            print(ip)
            s_logger.info(hostname + ":IP address: {}".format(ip))
        s_logger.info(hostname + ":Took {} seconds to find correct IP".format(round(time.time()-start)))
        
        # reconnect DB
        self.db.connect()

        # restart instaloader
        del self.L
        del self.profile
        self.L = instaloader.Instaloader()

def get_logger():
    """
    Return different loggers which are set according to which slack channels the log should go
    
    Return:
        logger object
    """
    logs = []
    for hook in [CRITICAL_WEBHOOK,SUMMARY_WEBHOOK,TIMING_WEBHOOK]:
        logger = logging.getLogger(hook)
        logger.setLevel(logging.INFO)
        h = SlackHandler(hook_url=hook)
        h.formatter = SlackFormatter()
        h.setLevel(logging.INFO)
        logger.addHandler(h)
        logs.append(logger)
    return logs[0], logs[1], logs[2]

def main():
    """
    Command line interaction
    """

    # creater parser
    parser = argparse.ArgumentParser(description = 'Instagram Scraper')
    parser.add_argument('--crawl', action = 'store_true', help = 'Crawl Users in DB to get more users')
    parser.add_argument('--update', action = 'store_true', help = "Update userMeta and postMeta of users who are above min_follower")
    parser.add_argument('--max_days',default = MAX_DAYS, help = 'Maximum age of posts to see who like/comment on a target previously')
    parser.add_argument('--min_follower',default = MIN_FOLLOWER, help = 'Minimum followers to download posts')
    parser.add_argument('--split_users',default = SPLIT_USERS, help = 'Whether to split influencers/users in Incomplete according to DO instances')
    parser.add_argument('--test', action = 'store_true', help = 'Start test function')
    args = parser.parse_args()
    
    # create Scraper object
    app = Scraper(**vars(args))

    # parse input and run accordingly
    app.crawl_post() if args.crawl else None
    app.update() if args.update else None
    test(**vars(args)) if args.test else None

def test(**kwargs):
    """
    Random test function
    """
    #L = instaloader.Instaloader()
    
    # Getting tagged users from users
    #profile = instaloader.Profile.from_username(L.context,'dreachong')
    #for post in islice(profile.get_posts(),100):
    #    print(post.tagged_users)
    
    # Getting tagged users from posts
    #for post in islice(L.get_hashtag_posts('singapore'),100):
    #    print(post.tagged_users)
    
    #app = Scraper(**kwargs)
    #r = app.db.get_date()
if __name__ == "__main__":
    c_logger, s_logger, t_logger = get_logger()
    hostname = str(socket.gethostname())
    main()

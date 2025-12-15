"""
Reddit data source collector using PRAW (Python Reddit API Wrapper).
"""

import praw
from datetime import datetime, timedelta
from typing import List, Dict, Any
import json
from .base_source import BaseSource, SourceCollectionError


class RedditSource(BaseSource):
    """
    Collects mentions from Reddit using the official API.
    
    Config requirements:
    - client_id: Reddit app client ID
    - client_secret: Reddit app client secret
    - user_agent: Reddit app user agent
    """
    
    def __init__(self, source_id: int, source_name: str, config: Dict[str, Any] = None):
        super().__init__(source_id, source_name, config)
        
        # Initialize Reddit API client
        try:
            self.reddit = praw.Reddit(
                client_id=config.get('client_id', ''),
                client_secret=config.get('client_secret', ''),
                user_agent=config.get('user_agent', 'SocialSentimentTracker/1.0')
            )
            self.reddit.read_only = True
        except Exception as e:
            raise SourceCollectionError(f"Failed to initialize Reddit client: {str(e)}")
    
    def collect(self, topic: Dict[str, Any], since: datetime = None) -> List[Dict[str, Any]]:
        """
        Collect Reddit posts and comments mentioning the topic.
        
        Searches across multiple subreddits relevant to the topic type.
        """
        if not since:
            since = datetime.now() - timedelta(hours=24)
        
        mentions = []
        keywords = json.loads(topic.get('keywords', '[]'))
        
        if not keywords:
            return mentions
        
        # Determine subreddits based on topic type
        subreddits = self._get_relevant_subreddits(topic.get('type', 'topic'))
        
        try:
            for keyword in keywords:
                for subreddit_name in subreddits:
                    try:
                        subreddit = self.reddit.subreddit(subreddit_name)
                        
                        # Search for posts
                        for submission in subreddit.search(
                            keyword, 
                            time_filter='day', 
                            limit=50,
                            sort='new'
                        ):
                            # Check if post is after 'since' date
                            post_time = datetime.fromtimestamp(submission.created_utc)
                            if post_time < since:
                                continue
                            
                            # Add post
                            mention = {
                                'text': f"{submission.title} {submission.selftext}",
                                'url': f"https://reddit.com{submission.permalink}",
                                'author': str(submission.author) if submission.author else '[deleted]',
                                'post_id': f"reddit_{submission.id}",
                                'posted_at': post_time,
                                'engagement_score': submission.score + submission.num_comments
                            }
                            
                            if self.validate_mention(mention):
                                mentions.append(self.normalize_mention(mention))
                            
                            # Also collect top comments
                            submission.comments.replace_more(limit=0)
                            for comment in submission.comments.list()[:10]:  # Top 10 comments
                                comment_time = datetime.fromtimestamp(comment.created_utc)
                                if comment_time < since:
                                    continue
                                
                                comment_mention = {
                                    'text': comment.body,
                                    'url': f"https://reddit.com{comment.permalink}",
                                    'author': str(comment.author) if comment.author else '[deleted]',
                                    'post_id': f"reddit_{comment.id}",
                                    'posted_at': comment_time,
                                    'engagement_score': comment.score
                                }
                                
                                if self.validate_mention(comment_mention):
                                    mentions.append(self.normalize_mention(comment_mention))
                    
                    except Exception as e:
                        print(f"Error collecting from r/{subreddit_name}: {str(e)}")
                        continue
        
        except Exception as e:
            raise SourceCollectionError(f"Reddit collection failed: {str(e)}")
        
        return mentions
    
    def _get_relevant_subreddits(self, topic_type: str) -> List[str]:
        """
        Return relevant subreddits based on topic type.
        This can be customized in the config.
        """
        default_subreddits = {
            'stock': ['wallstreetbets', 'stocks', 'investing', 'stockmarket', 'options'],
            'topic': ['technology', 'Futurology', 'science', 'news'],
            'keyword': ['all']  # searches all of reddit
        }
        
        # Allow custom subreddits from config
        custom_subreddits = self.config.get('subreddits', [])
        if custom_subreddits:
            return custom_subreddits
        
        return default_subreddits.get(topic_type, ['all'])

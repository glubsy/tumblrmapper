import urllib
from requests import Session
from yurl import URL


def tumdlr_post_downloader(ctx, url, images, videos):
    """Download posts from a Tumblr account."""
    log = logging.getLogger('tumdlr.commands.downloader')
    log.info('Starting a new download session for %s', url)

    # Get our post information
    tumblr = TumblrBlog(url)
    progress = 0

    for post in tumblr.posts():  # type: TumblrPost
        # Generic data
        progress_data = OrderedDict([
            ('Progress', '{cur} / {total} posts processed'.format(cur=progress, total=tumblr.post_count)),
            ('Type', post.type.title()),
            ('Post Date', post.post_date),
            ('Tags', post.tags)
        ])

        session = Session()
        session.headers.update({'referer': urllib.parse.quote(post.url.as_string())})

        
        for file in post.files:
            #TODO: only download if post is found in table ARCHIVES_1280
            try:
                file.download(ctx, session=session, progress_data=progress_data)
            except TumdlrDownloadError:
                click.echo('File download failed, skipping', err=True)

        progress += 1


class TumblrBlog:

    def __init__(self, url, session=None, **kwargs):
        """
        Tumblr blog

        Args:
            url(URL|str): Tumblr profile URL
            session(Optional[Session]): An optional custom Requests session

        Keyword Args:
            api_key(str): Tumblr API key
            uagent(str): Custom User-Agent header
        """
        self._url = url if isinstance(url, URL) else URL(url)
        self._api_url = URL(scheme='https', host='api.tumblr.com', path='/v2/')
        self._api_response = None  # type: Response
        self._api_key = kwargs.get('api_key', 'fuiKNFp9vQFvjLNvx4sUwti4Yb5yGutBN4Xh10LXZhhRKjWlV4')
        self._uagent = kwargs.get('user_agent', 'tumdlr/{version}')

        if not session:
            session = Session()
            session.headers.update({
                'Referer': urllib.parse.quote(self._url.as_string()),
                'User-Agent': self._uagent
            })

        self.session = session

        self.title          = None  # type: str
        self.url            = None  # type: URL
        self.name           = None  # type: str
        self.description    = None  # type: str
        self.is_nsfw        = None  # type: bool
        self.likes          = None  # type: int|False
        self.post_count     = None  # type: int
        self.updated        = None  # type: int

        self._posts = []
        self.offset = 0

        self._api_url = self._api_url.replace(
            path=self._api_url.path + 'blog/{host}/posts'.format(host=self._url.host)
        )
        self._api_get()

    def _api_get(self, query=None, parse=True):
        """
        Execute an API query

        Args:
            query(Optional[dict]): Extra query parameters
            parse(Optional[bool]): Parse the API response immediately
        """
        # Parse extra query parameters
        query_extra = []

        if query:
            for key, value in query.items():
                query_extra.append(
                    '{key}={value}'.format(
                        key=urllib.parse.quote(key),
                        value=urllib.parse.quote(value)
                    )
                )

        # Only prepend an ampersand if we have extra attributes, otherwise default to an empty string
        if query_extra:
            query_extra = '&' + '&'.join(query_extra)
        else:
            query_extra = ''

        endpoint = self._api_url.replace(
            query='api_key={api_key}&filter=text&offset={offset}{extra}'.format(
                api_key=self._api_key, offset=self.offset, extra=query_extra
            )
        )

        response = self.session.get(endpoint.as_string())  # type: Response
        response.raise_for_status()

        self._api_response = response
        if parse:
            self._api_parse_response()

    def _api_parse_response(self):
        """ Parse an API response  """
        blog = self._api_response.json()['response']['blog']

        self.title          = blog['title']
        self.url            = URL(blog['url'])
        self.name           = blog['name']
        self.description    = blog['description']
        self.is_nsfw        = blog['is_nsfw']
        self.likes          = blog.get('likes', False)  # Returned only if sharing of likes is enabled
        self.post_count     = blog['posts']
        self.updated        = blog['updated']

        posts = self._api_response.json()['response']['posts']

        for post in posts:
            try:
                if post['type'] in ['photo', 'link']:
                    self._posts.append(TumblrPhotoSet(post, self))
                    continue
                elif post['type'] == 'video':
                    self._posts.append(TumblrVideoPost(post, self))
                    continue

                self._posts.append(TumblrPost(post, self))
            except TumdlrParserError:
                continue

    def posts(self):
        """
        Yields:
            TumblrPost
        """
        while True:
            # Out of posts?
            if not self._posts:
                # Do we have any more to query?
                self._api_get()

                if not self._posts:
                    # Nope, we've queried everything, break now
                    break

            # Pop our next post and increment the offset
            post = self._posts.pop(0)
            self.offset += 1

            yield post












class TumblrPost:
    """
    This is the base container class for all Tumblr post types. It contains data that is always available with any
    type of post.

    Additional supported post types may extend this class to provide additional metadata parsing
    """
    def __init__(self, post, blog):
        """
        Args:
            post(dict): API response
            blog(tumdlr.api.TumblrBlog): Parent blog
        """
        self._post = post
        self.blog = blog
        self.log = logging.getLogger('tumdlr.containers.post')

        self.id         = None  # type: int
        self.type       = None  # type: str
        self.url        = None  # type: URL
        self.tags       = set()
        self.post_date  = None  # type: str
        self.note_count = None  # type: int

        self.files = []

        try:
            self._parse_post()
        except Exception as e:
            self.log.warn('Failed to parse post data: %r', self, exc_info=e)
            raise TumdlrParserError(post_data=post)

    @property
    def is_text(self):
        """
        Returns:
            bool
        """
        return self.type == 'text'

    @property
    def is_photo(self):
        """
        Returns:
            bool
        """
        return self.type in ['photo', 'link']

    @property
    def is_video(self):
        """
        Returns:
            bool
        """
        return self.type == 'video'

    def _parse_post(self):
        self.id         = self._post['id']
        self.type       = self._post['type']
        self.url        = URL(self._post['post_url']) if 'post_url' in self._post else None
        self.tags       = set(self._post.get('tags', []))
        self.note_count = self._post.get('note_count')
        self.post_date  = self._post['date']

    def __repr__(self):
        return "<TumblrPost id='{id}' type='{type}' url='{url}'>"\
            .format(id=self.id, type=self.type, url=self.url)

    def __str__(self):
        return self.url.as_string() if self.url else ''


class TumblrPhotoSet(TumblrPost):
    """
    Container class for Photo and Photo Link post types
    """
    def __init__(self, post, blog):
        """
        Args:
            post(dict): API response
            blog(tumdlr.api.blog.TumblrBlog): Parent blog
        """
        self.log = logging.getLogger('tumdlr.containers.post')
        super().__init__(post, blog)

    def _parse_post(self):
        """
        Parse all available photos using the best image sizes available
        """
        super()._parse_post()
        self.title  = self._post.get('caption', self._post.get('title'))

        photos = self._post.get('photos', [])
        is_photoset = (len(photos) > 1)

        for page_no, photo in enumerate(photos, 1):
            best_size = photo.get('original_size') or max(photo['alt_sizes'], key='width')
            best_size['page_no'] = page_no if is_photoset else False
            self.files.append(TumblrPhoto(best_size, self))

    def __repr__(self):
        return "<TumblrPhotoSet title='{title}' id='{id}' photos='{count}'>"\
            .format(title=self.title.split("\n")[0].strip(), id=self.id, count=len(self.files))


class TumblrVideoPost(TumblrPost):
    """
    Container class for Video post types
    """
    def __init__(self, post, blog):
        """
        Args:
            post(dict): API response
            blog(tumdlr.api.blog.TumblrBlog): Parent blog
        """
        self.log = logging.getLogger('tumdlr.containers.post')

        self.title          = None
        self.description    = None
        self.duration       = None
        self.format         = None

        super().__init__(post, blog)

    def _parse_post(self):
        """
        Parse all available photos using the best image sizes available
        """
        super()._parse_post()

        video_info = YoutubeDL().extract_info(self.url.as_string(), False)

        self.title = video_info.get('title')

        self.description    = video_info.get('description')
        self.duration       = int(video_info.get('duration', 0))
        self.format         = video_info.get('format', 'Unknown')

        self.files.append(TumblrVideo(video_info, self))

    def __repr__(self):
        return "<TumblrVideoPost id='{id}'>".format(id=self.id)


class TumblrFile:
    """
    This is the base container class for all downloadable resources associated with Tumblr posts.
    """

    CATEGORY = 'misc'

    def __init__(self, data, container):
        """
        Args:
            data(dict): API response data
            container(TumblrPost): Parent container
        """
        self.log = logging.getLogger('tumdlr.containers.file')

        self._data      = data
        self.container  = container
        self.url        = URL(self._data.get('url', self._data.get('post_url')))

    def download(self, context, **kwargs):
        """
        Args:
            context(tumdlr.main.Context): CLI request context
            kwargs(dict): Additional arguments to send with the download request

        Returns:
            str: Path to the saved file
        """
        try:
            download(self.url.as_string(), str(self.filepath(context, kwargs)), **kwargs)
        except Exception as e:
            self.log.warn('Post download failed: %r', self, exc_info=e)
            raise TumdlrDownloadError(error_message=str(e), download_url=self.url.as_string())

    def filepath(self, context, request_data):
        """
        Args:
            context(tumdlr.main.Context): CLI request context
            request_data(Optional[dict]): Additional arguments to send with the download request

        Returns:
            Path
        """
        # Construct the save basedir
        basedir = Path(context.config['Tumdlr']['SavePath'])

        # Are we categorizing by user?
        if context.config['Categorization']['User']:
            self.log.debug('Categorizing by user: %s', self.container.blog.name)
            basedir = basedir.joinpath(sanitize_filename(self.container.blog.name))

        # Are we categorizing by post type?
        if context.config['Categorization']['PostType']:
            self.log.debug('Categorizing by type: %s', self.CATEGORY)
            basedir = basedir.joinpath(self.CATEGORY)

        self.log.debug('Basedir constructed: %s', basedir)

        return basedir


class TumblrPhoto(TumblrFile):

    CATEGORY = 'photos'

    def __init__(self, photo, photoset):
        """
        Args:
            photo(dict): Photo API data
            photoset(TumblrPhotoSet): Parent container
        """
        super().__init__(photo, photoset)

        self.width   = self._data.get('width')
        self.height  = self._data.get('height')
        self.page_no = self._data.get('page_no', False)

    def filepath(self, context, request_data):
        """
        Get the full file path to save the downloaded file to

        Args:
            context(tumdlr.main.Context): CLI request context
            request_data(Optional[dict]): Additional arguments to send with the download request

        Returns:
            Path
        """
        assert isinstance(self.container, TumblrPhotoSet)
        filepath = super().filepath(context, request_data)

        request_data['progress_data']['Caption'] = self.container.title

        # Are we categorizing by photosets?
        if self.page_no and context.config['Categorization']['Photosets']:
            self.log.debug('Categorizing by photoset: %s', self.container.id)
            filepath = filepath.joinpath(sanitize_filename(str(self.container.id)))

        # Prepend the page number for photosets
        if self.page_no:
            filepath = filepath.joinpath(sanitize_filename('p{pn}_{pt}'.format(pn=self.page_no,
                                                                               pt=self.container.title)))
            request_data['progress_data']['Photoset Page'] = '{cur} / {tot}'\
                .format(cur=self.page_no, tot=len(self.container.files))
        else:
            filepath = filepath.joinpath(sanitize_filename(self.container.title))

        # Work out the file extension and return
        return str(filepath) + os.path.splitext(self.url.as_string())[1]

    def __repr__(self):
        return "<TumblrPhoto url='{url}' width='{w}' height='{h}'>".format(url=self.url, w=self.width, h=self.height)

    def __str__(self):
        return self.url.as_string()


class TumblrVideo(TumblrFile):

    CATEGORY = 'videos'

    def __init__(self, video, vpost):
        """
        Args:
            video(dict): Video API data
            vpost(TumblrVideoPost): Parent container
        """
        super().__init__(video, vpost)

    def filepath(self, context, request_data):
        """
        Get the full file path to save the video to

        Args:
            context(tumdlr.main.Context): CLI request context
            request_data(Optional[dict]): Additional arguments to send with the download request

        Returns:
            Path
        """
        assert isinstance(self.container, TumblrVideoPost)
        filepath = super().filepath(context, request_data)

        minutes  = int(self.container.duration / 60)
        seconds  = self.container.duration % 60
        duration = '{} minutes {} seconds'.format(minutes, seconds) if minutes else '{} seconds'.format(seconds)

        if self.container.title:
            request_data['progress_data']['Title'] = self.container.title

        request_data['progress_data']['Description'] = self.container.description
        request_data['progress_data']['Duration'] = duration
        request_data['progress_data']['Format'] = self.container.format

        filepath = filepath.joinpath(sanitize_filename(
            self.container.description or
            md5(self.url.as_string().encode('utf-8')).hexdigest())
        )

        # Work out the file extension and return
        return '{}.{}'.format(str(filepath), self._data.get('ext', 'mp4'))

    def __repr__(self):
        return "<TumblrVideo id='{i}'>".format(i=self.container.id)

    def __str__(self):
        return self.url.as_string()


























class TumdlrException(Exception):
    pass


###############################
# Begin generic errors        #
###############################

class TumdlrParserError(TumdlrException):
    def __init__(self, *args, **kwargs):
        self.post_data = kwargs.get('post_data')
        super().__init__('An error occurred while parsing a posts API response data')


###############################
# Begin file container errors #
###############################
class TumdlrFileError(TumdlrException):
    pass


class TumdlrDownloadError(TumdlrFileError):
    def __init__(self, *args, **kwargs):
        self.download_url   = kwargs.get('download_url')
        self.error_message  = kwargs.get('error_message')
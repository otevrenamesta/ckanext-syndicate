import logging
from urlparse import urlparse
import ckan
import ckanapi
import os
import routes

from pylons import config

import ckan.plugins.toolkit as toolkit
from ckan.lib.helpers import get_pkg_dict_extra

import requests
import ckan.lib.uploader as uploader

logger = logging.getLogger(__name__)

def get_syndicate_flag():
    return config.get('ckan.syndicate.flag', 'syndicate')


def get_syndicated_id():
    return config.get('ckan.syndicate.id', 'syndicated_id')


def get_syndicated_author():
    return config.get('ckan.syndicate.author')


def get_syndicated_name_prefix():
    return config.get('ckan.syndicate.name_prefix', '')


def get_syndicated_organization():
    return config.get('ckan.syndicate.organization', None)


def is_organization_preserved():
    return toolkit.asbool(config.get('ckan.syndicate.replicate_organization', False))


# ----------- #
# entry point #
# ----------- #

def sync_package_task(package, action, ckan_ini_filepath):
    logger = sync_package_task.get_logger()
    load_config(ckan_ini_filepath)
    register_translator()
    logger.info("Sync package %s, with action %s" % (package, action))
    return sync_package(package, action)


logger = logging.getLogger(__name__)


def get_logger():
    return logger
sync_package_task.get_logger = get_logger

def remove_items(package):
    to_remove = ['id', 'md_state', 'md_sharing_level', 'md_syndicate',
                 'md_syndicated_id', 'md_gdpr', 'md_primary_source']
    for f in to_remove:
       if f in package:
          del package[f]

def load_config(ckan_ini_filepath):
    import paste.deploy
    config_abs_path = os.path.abspath(ckan_ini_filepath)
    conf = paste.deploy.appconfig('config:' + config_abs_path)
    import ckan
    ckan.config.environment.load_environment(conf.global_conf,
                                             conf.local_conf)

    ## give routes enough information to run url_for
    parsed = urlparse(conf.get('ckan.site_url', 'http://0.0.0.0'))
    request_config = routes.request_config()
    request_config.host = parsed.netloc + parsed.path
    request_config.protocol = parsed.scheme


def register_translator():
    # https://github.com/ckan/ckanext-archiver/blob/master/ckanext/archiver/bin/common.py
    # If not set (in cli access), patch the a translator with a mock, so the
    # _() functions in logic layer don't cause failure.
    from paste.registry import Registry
    from pylons import translator
    from ckan.lib.cli import MockTranslator
    if 'registery' not in globals():
        global registry
        registry = Registry()
        registry.prepare()

    if 'translator_obj' not in globals():
        global translator_obj
        translator_obj = MockTranslator()
        registry.register(translator, translator_obj)


def get_target():
    if hasattr(get_target, 'ckan'):
        return get_target.ckan
    ckan_url = config.get('ckan.syndicate.ckan_url')
    api_key = config.get('ckan.syndicate.api_key')
    user_agent = config.get('ckan.syndicate.user_agent', None)
    assert ckan_url and api_key, "Task must have ckan_url and api_key"

    ckan = ckanapi.RemoteCKAN(ckan_url, apikey=api_key, user_agent=user_agent)

    get_target.ckan = ckan
    return ckan


def filter_resources(resources):
    '''
    Drop hash from resources
    '''
    res = resources[:]

    for r in res:
        if 'hash' in r:
            r.pop('hash')

    return res

def sync_package(package_id, action, ckan_ini_filepath=None):
    logger.info('sync package {0}'.format(package_id))

    # load the package at run of time task (rather than use package state at
    # time of task creation).
    from ckan import model
    context = {'model': model, 'ignore_auth': True, 'session': model.Session,
               'use_cache': False, 'validate': True}  #LN False > True

    params = {
        'id': package_id,
    }
    package = toolkit.get_action('package_show')(
        context,
        params,
    )
    if action == 'dataset/create':
        _create_package(package)

    elif action == 'dataset/update':
        _update_package(package)
    else:
        raise Exception('Unsupported action {0}'.format(action))


def replicate_remote_organization(org):
    ckan = get_target()

    try:
        remote_org = ckan.action.organization_show(id=org['name'])
    except toolkit.ObjectNotFound:
        org.pop('image_url')
        org.pop('id')
        remote_org = ckan.action.organization_create(**org)

    return remote_org['id']


def _create_package(package):

    rem_ckan = get_target()

    # Create a new package based on the local instance
    new_package_data = dict(package)
    #del new_package_data['id']
    remove_items(new_package_data)

    # don't sync md_ticket_url as public and private instances
    # could have different trackers
    if 'md_ticket_url' in new_package_data:
        del new_package_data['md_ticket_url']

    format_name = "%s%s"
    if get_syndicated_name_prefix():
        format_name = "%s-%s"
    new_package_data['name'] = format_name % (
        get_syndicated_name_prefix(),
        new_package_data['name'])
    logger.info('_create_package: name=' +new_package_data['name'])

    if 'resources' in package:
        new_package_data['resources'] = filter_resources(package['resources'])

    if (new_package_data.pop('type') == 'dataset'):
      org = new_package_data.pop('organization')

      if is_organization_preserved():
          org_id = replicate_remote_organization(org)
      else:
          org_id = get_syndicated_organization()

      new_package_data['owner_org'] = org_id

    try:
        # TODO: No automated test
        new_package_data = toolkit.get_action('update_dataset_for_syndication')(
            {}, {'dataset_dict': new_package_data})
    except KeyError:
        pass

    try:
        #logging.info("np {}".format(new_package_data))

        remote_package = rem_ckan.action.package_create(**new_package_data)

        # XXX: WTF
        #remote_package = rem_ckan.action.ckanext_showcase_create(**new_package_data)
        #logging.info("rp {}".format(remote_package))

        set_syndicated_id(package, remote_package['id'])
        logging.info("Done")
    except toolkit.ValidationError as e:  #at target instance
        #0/0
        #if u'Toto URL je ji\u017e pou\u017e\xedv\xe1no.' or 'That URL is already in use.' in e.error_dict.get('name', []):
        if u'Validation Error' in e.error_dict.get('__type', []):
            logger.info("package with name '{0}' already exists. Check creator.".format(
                new_package_data['name']))
            author = get_syndicated_author()
            if author is None:
                raise
            try:
                remote_package = rem_ckan.action.package_show(
                    id=new_package_data['name'])
                remote_user = rem_ckan.action.user_show(id=author)
            except toolkit.ValidationError as e:
                log.error(e.errors)
                raise
            except toolkit.ObjectNotFound as e:
                log.error('User "{0}" not found'.format(author))
                raise
            else:
                if remote_package['creator_user_id'] == remote_user['id']:
                    logger.info("Author is the same({0}). Updating".format(
                        author))

                    res = rem_ckan.action.package_update(
                        id=remote_package['id'],
                        **new_package_data
                    )

                    logger.debug("package_update result {}".format(res))

                    set_syndicated_id(package, remote_package['id'])

                    if 'resources' in new_package_data:
                        for r in new_package_data['resources']:
                            self.upload_resource(r)
                else:
                    logger.info(
                        "Creator of remote package '{0}' did not match '{1}'. Skipping".format(
                            remote_user['name'], author))


def _update_package(package):

    syndicated_id = None
    if get_syndicated_id() in package:
        #syndicated_id = get_pkg_dict_extra(package, get_syndicated_id())
        syndicated_id = package[get_syndicated_id()]

    if syndicated_id is None or str(syndicated_id) == '':
        logging.info("syndication ID not found, creating package")
        _create_package(package)
        return

    ckan = get_target()

    try:
        updated_package = dict(package)

        if not toolkit.asbool(updated_package[get_syndicate_flag()]) and updated_package[get_syndicated_id()] != "":
            updated_package['state'] = 'deleted'

        # Keep the existing remote ID and Name
        #del updated_package['id']
        del updated_package['name']
        remove_items(updated_package)

        rem_ckan = get_target()
        remote_package = rem_ckan.action.package_show(id=package['name'])

        if 'resources' in package:
           updated_package['resources'] = filter_resources(package['resources'])

        org = updated_package.pop('organization')

        if is_organization_preserved():
            org_id = replicate_remote_organization(org)
        else:
            org_id = get_syndicated_organization()

        updated_package['owner_org'] = org_id

        try:
            # TODO: No automated test
            updated_package = toolkit.get_action(
                'update_dataset_for_syndication')(
                {}, {'dataset_dict': updated_package})
        except KeyError:
            pass

        ckan.action.package_update(
            id=syndicated_id,
            **updated_package
        )

        if 'resources' in updated_package:
            for r in updated_package['resources']:
              upload_resource(r)

    except ckanapi.NotFound:
        _create_package(package)

def upload_resource(resource):
    logging.info("Got resource syndicate")
    if resource['url_type'] != 'upload':
      logging.info("Not upload url_type, skipping")
      return
    ckan_url = config.get('ckan.syndicate.ckan_url')
    api_key = config.get('ckan.syndicate.api_key')
    logging.info("Uploading resource {}".format(resource['id']))
    url = os.path.join(ckan_url, 'api/action/resource_update')
    logging.info("TO {}".format(url))

    fileurl = resource['url']
    filename = os.path.basename(fileurl)

    upload = uploader.get_resource_uploader(resource)
    res = requests.post(url,
                        data={'id': resource['id']},
                        headers = {"X-CKAN-API-Key": api_key},
                        files=[('upload', (filename, file(upload.get_path(resource['id']))))]) # all your parenthesis

    logging.info("{}".format(res))

def set_syndicated_id(local_package, remote_id):
    local_package[get_syndicated_id()] = remote_id
    _update_local_package(local_package)
    # XXX: we should probably call udpate_search_index and update_package_extras
    # as well or not??
    #_update_search_index(package_obj.id, logger)

def _update_local_package(package):
    site_user = ckan.logic.get_action('get_site_user')({
            'model': ckan.model,
            'ignore_auth': True},
            {}
      )

    context = {'model': ckan.model, 'ignore_auth': True, 'session': ckan.model.Session,
               'use_cache': False, 'validate': True, 'user': site_user['name']}

    toolkit.get_action('package_update')(context, package)


def _update_package_extras(package):
    from ckan import model
    from ckan.lib.dictization.model_save import package_extras_save

    package_id = package['id']
    package_obj = model.Package.get(package_id)
    if not package:
        raise Exception('No Package with ID %s found:s' % package_id)

    extra_dicts = package.get("extras")
    context_ = {'model': model, 'session': model.Session}
    model.repo.new_revision()
    package_extras_save(extra_dicts, package_obj, context_)
    model.Session.commit()
    model.Session.flush()

    _update_search_index(package_obj.id, logger)


def _update_search_index(package_id, log):
    '''
    Tells CKAN to update its search index for a given package.
    '''
    from ckan import model
    from ckan.lib.search.index import PackageSearchIndex
    package_index = PackageSearchIndex()
    context_ = {'model': model, 'ignore_auth': True, 'session': model.Session,
                'use_cache': False, 'validate': True}   #LN False > True
    package = toolkit.get_action('package_show')(context_, {'id': package_id})
    package_index.index_package(package, defer_commit=False)
    log.info('Search indexed %s', package['name'])

# -*- coding: utf-8 -*-

import os

import ckan.plugins as plugins
import ckan.plugins.toolkit as toolkit
import ckan.lib.jobs as jobs

from pylons import config
import ckan.model as model
from ckan.model.domain_object import DomainObjectOperation

from ckanext.syndicate.tasks import (get_syndicated_id,
                                     get_syndicate_flag,
                                     sync_package_task)


def syndicate_dataset(package_id, topic):
    ckan_ini_filepath = os.path.abspath(config['__file__'])
    jobs.enqueue(sync_package_task, [package_id, topic, ckan_ini_filepath])


class SyndicatePlugin(plugins.SingletonPlugin):
    plugins.implements(plugins.IConfigurer)
    plugins.implements(plugins.IDomainObjectModification, inherit=True)

    # IConfigurer

    ## Based on ckanext-webhooks plugin
    # IDomainObjectNotification & IResourceURLChange
    def notify(self, entity, operation=None):
        if not operation:
            # This happens on IResourceURLChange
            return

        if isinstance(entity, model.Package):
            self._syndicate_dataset(entity, operation)

    def update_config(self, config_):
        pass

    def _syndicate_dataset(self, dataset, operation):
        topic = self._get_topic('dataset', operation)

        #if topic is not None and self._syndicate(dataset):
        #XXX: required?
        if topic is not None and (self._syndicate(dataset) or dataset.extras.get(get_syndicated_id()) != ''):
            syndicate_dataset(dataset.id, topic)

    def _syndicate(self, dataset):
        return (not dataset.private and
                toolkit.asbool(dataset.extras.get(get_syndicate_flag(), 'false')))

    def _get_topic(self, prefix, operation):
        topics = {
            DomainObjectOperation.new: 'create',
            DomainObjectOperation.changed: 'update',
        }

        topic = topics.get(operation, None)

        if topic is not None:
            return '{0}/{1}'.format(prefix, topic)

        return None

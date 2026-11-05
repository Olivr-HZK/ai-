"""Draft-only source entry: real journal transitions, no Replica transport."""
import copy
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from ua_workflows import replica_request_entry as entry
from ua_workflows.replica_request_pool import TABLE


class Feishu:
    def __init__(self):
        self.source = {'record_id': 'recSource', 'fields': {entry.TYPE: '替换logo', entry.TRIGGER: False, '广告主': 'Fixture'}}
        self.demands, self.posts, self.writes = {}, [], []
        self.lose_create = self.lose_ack = self.lose_claim = False
        self.hide_created = False
        self.generic_links = True

    def listing(self, route):
        if '/tables/' + TABLE + '/' in route:
            types = {entry.TYPE: 3, entry.LANG: 4, entry.PORTRAIT: 17, entry.TRIGGER: 7,
                     entry.CLAIM: 1, entry.LINKS: 18, entry.NOTE: 1}
            return [{'field_id': 'id-' + name, 'field_name': name, 'type': kind,
                     'property': {'table_id': 'tblPool', 'multiple': True} if name == entry.LINKS else {}}
                    for name, kind in types.items()]
        return [{'field_id': 'marker-id', 'field_name': entry.MARKER, 'type': 1}]

    def call(self, method, route, **kwargs):
        source = '/tables/' + TABLE + '/' in route
        if method == 'GET':
            rid = route.rsplit('/', 1)[-1]
            return {'record': copy.deepcopy(self.source if source else self.demands[rid])}
        if route.endswith('/search'):
            c = kwargs['json']['filter']['conditions'][0]
            if source:
                assert c == {'field_name': entry.TRIGGER, 'operator': 'is', 'value': ['true']}
                return {'items': [copy.deepcopy(self.source)] if self.source['fields'].get(entry.TRIGGER) else []}
            return {'items': [] if self.hide_created else [copy.deepcopy(r) for r in self.demands.values()
                    if r['fields'][entry.MARKER] == c['value'][0]]}
        if route.endswith('/batch_create'):
            self.posts.append(copy.deepcopy(kwargs))
            fields = copy.deepcopy(kwargs['json']['records'][0]['fields'])
            rid = 'recDraft' + str(len(self.demands) + 1)
            self.demands[rid] = {'record_id': rid, 'fields': fields}
            if self.lose_create:
                raise OSError('Response lost after server accepted')
            return {'records': [copy.deepcopy(self.demands[rid])]}
        if method == 'PUT' and source:
            fields = copy.deepcopy(kwargs['json']['fields'])
            assert set(fields) <= {entry.CLAIM, entry.LINKS, entry.TRIGGER, entry.NOTE}
            self.writes.append(fields)
            if entry.LINKS in fields and self.generic_links:
                fields[entry.LINKS] = [{'record_ids': fields[entry.LINKS], 'table_id': 'tblPool', 'type': 'text', 'text': 'Titles'}]
            self.source['fields'].update(fields)
            if self.lose_claim and entry.CLAIM in fields:
                raise OSError('Claim receipt lost')
            if self.lose_ack and entry.LINKS in fields:
                raise OSError('Ack receipt lost')
            return {'record': copy.deepcopy(self.source)}
        raise AssertionError((method, route))


class EntryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.root = Path(self.temp.name)
        self.client = Feishu()
        names = [entry.TYPE, entry.LANG, entry.PORTRAIT, entry.TRIGGER, entry.CLAIM, entry.LINKS, entry.NOTE]
        self.settings = {'request_table_id': 'tblPool', 'source_entry': {
            'fields': {n: 'id-' + n for n in names}, 'marker_field_id': 'marker-id'}}
        self.processor = entry.SourceEntry(self.client, self.settings, self.root)

    def tearDown(self): self.temp.cleanup()

    def stage(self): return json.loads(next(self.root.glob('*.json')).read_text())['stage']

    def test_unchecked_source_never_creates(self):
        self.processor.tick()
        self.assertEqual(self.client.posts, [])
        self.assertEqual(self.client.writes, [])

    def test_stale_checked_search_result_is_not_a_new_manual_click(self):
        stale = copy.deepcopy(self.client.source)
        stale['fields'][entry.TRIGGER] = True
        with patch.object(self.processor, 'search', return_value=[stale]):
            self.processor.tick()
        self.assertEqual(self.client.posts, [])
        self.assertEqual(self.client.writes, [])

    def test_checked_source_creates_only_draft_with_real_link_and_no_repeat(self):
        self.client.source['fields'][entry.TRIGGER] = True
        self.processor.tick(); self.processor.tick()
        self.assertEqual(len(self.client.posts), 1)
        fields = self.client.demands['recDraft1']['fields']
        self.assertEqual(fields['需求状态'], '新建')
        self.assertEqual(fields['关联原素材'], ['recSource'])
        self.assertEqual(fields['目标品牌'], 'Afizzy')
        self.assertEqual(self.stage(), 'complete')
        self.assertFalse(self.client.source['fields'][entry.TRIGGER])
        self.assertEqual(entry.linked_ids(self.client.source['fields'][entry.LINKS], 'tblPool'), ['recDraft1'])

    def test_new_manual_click_creates_second_draft_and_preserves_first_link(self):
        self.client.source['fields'][entry.TRIGGER] = True
        self.processor.tick()
        self.client.source['fields'][entry.TRIGGER] = True
        self.processor.tick()
        self.assertEqual(len(self.client.posts), 2)
        self.assertNotEqual(self.client.posts[0]['params']['client_token'], self.client.posts[1]['params']['client_token'])
        self.assertEqual(entry.linked_ids(self.client.source['fields'][entry.LINKS], 'tblPool'), ['recDraft1', 'recDraft2'])

    def test_missing_translation_or_portrait_inputs_do_not_create(self):
        f = self.client.source['fields']; f[entry.TRIGGER] = True
        for action in ['替换语言', '替换人物']:
            f[entry.TYPE] = action
            self.processor.tick()
            self.assertTrue(f[entry.TRIGGER])
        self.assertEqual(self.client.posts, [])

    def test_language_and_reference_tokens_are_copied_into_draft(self):
        row = copy.deepcopy(self.client.source); row['fields'].update({entry.TYPE: '替换语言', entry.LANG: ['日语']})
        self.assertEqual(self.processor.payload(row)['目标语言'], ['日语'])
        row['fields'].update({entry.TYPE: '替换人物', entry.PORTRAIT: [{'file_token': 'portrait', 'name': 'Person.png'}]})
        self.assertEqual(self.processor.payload(row)['人物参考图'], [{'file_token': 'portrait'}])

    def test_unknown_create_recovers_by_exact_marker_without_another_post(self):
        self.client.source['fields'][entry.TRIGGER] = True; self.client.lose_create = True
        self.processor.tick(); self.assertEqual(self.stage(), 'draft_unknown')
        self.processor.tick()
        self.assertEqual(self.stage(), 'complete')
        self.assertEqual(len(self.client.posts), 1)

    def test_unknown_create_without_remote_evidence_stays_unknown_across_restart(self):
        self.client.source['fields'][entry.TRIGGER] = True; self.client.lose_create = True; self.client.hide_created = True
        self.processor.tick()
        for _ in range(3):
            entry.SourceEntry(self.client, self.settings, self.root).tick()
        self.assertEqual(self.stage(), 'draft_unknown')
        self.assertEqual(len(self.client.posts), 1)

    def test_unknown_source_claim_reads_before_first_create(self):
        self.client.source['fields'][entry.TRIGGER] = True; self.client.lose_claim = True
        self.processor.tick(); self.assertEqual(self.client.posts, [])
        self.processor.tick()
        self.assertEqual(self.stage(), 'complete')
        self.assertEqual(len(self.client.posts), 1)

    def test_unknown_ack_reads_back_without_repeating_put(self):
        self.client.source['fields'][entry.TRIGGER] = True; self.client.lose_ack = True
        self.processor.tick(); self.assertEqual(self.stage(), 'ack_unknown')
        writes = len(self.client.writes)
        self.processor.tick()
        self.assertEqual(self.stage(), 'complete')
        self.assertEqual(len(self.client.writes), writes)

    def test_unknown_foreign_claim_is_never_reused(self):
        self.client.source['fields'].update({entry.TRIGGER: True, entry.CLAIM: 'foreign-claim'})
        self.processor.tick()
        self.assertEqual(self.client.posts, [])

    def test_wrong_target_table_in_link_is_rejected(self):
        with self.assertRaises(ValueError):
            entry.linked_ids([{'record_ids': ['recDraft1'], 'table_id': TABLE}], 'tblPool')

    def test_real_feishu_empty_relation_payload_is_empty(self):
        self.assertEqual(entry.linked_ids([{'record_ids': [], 'table_id': 'tblPool',
                                           'type': 'text', 'text': ''}], 'tblPool'), [])
        self.assertEqual(entry.linked_ids([{'table_id': 'tblPool', 'text_arr': [], 'type': 'text'}], 'tblPool'), [])


if __name__ == '__main__': unittest.main()

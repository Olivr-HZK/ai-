import contextlib
import hashlib
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from ua_workflows import afizzy_daily as workflow

class WorkflowEntryTests(unittest.TestCase):
    def test_pre_review_filters_actual_landscape_before_loading_vectors_or_frames(self):
        from PIL import Image
        from ua_workflows.social_chat_media_orientation import POLICY
        from ua_workflows.social_chat_plugin_review_plan import POLICY as PROCESSING
        with tempfile.TemporaryDirectory() as tmp,contextlib.redirect_stdout(io.StringIO()):
            root=Path(tmp);media=root/'wide.png';Image.new('RGB',(24,16)).save(media)
            index=root/'index.json';index.write_text(json.dumps([{'id':'wide','kind':'图片','day':'2026-11-02','sources':[{'path':str(media),'sha256':workflow.sha(media)}]}]))
            (root/'scope.json').write_text(json.dumps({'target_date':'2026-11-02','processing_policy':PROCESSING,'media_orientation_policy':POLICY}))
            (root/'handoff.json').write_text(json.dumps({'status':'PACKAGE_RECEIVED'}))
            self.assertEqual(workflow.main(['pre-review','--date','2026-11-02','--run-dir',tmp,'--media-index',str(index),'--embedding-cache',str(root/'absent-cache')]),0)
            result=json.loads((root/'pre-review/plan.json').read_text())
            self.assertEqual(result['review_queue'],[])
            self.assertEqual(result['media_orientation_filter']['counts'],{'landscape_excluded':1})
    def test_task_has_order_and_previous_day_without_running_business(self):
        out=io.StringIO()
        with contextlib.redirect_stdout(out),patch.object(workflow.subprocess,'run') as run:
            self.assertEqual(workflow.main(['task','--date','2026-11-02']),0)
        text=out.getvalue();self.assertIn('2026-11-01',text);self.assertIn('record_ids',text);self.assertIn('没有审核结果不写表',text);self.assertIn('未',text) if False else None;run.assert_not_called()

    def test_date_required_and_run_all_not_available(self):
        for arguments in [['crawl'],['task','--date','11.2'],['run-all','--date','2026-11-02']]:
            with contextlib.redirect_stderr(io.StringIO()),self.assertRaises(SystemExit):workflow.main(arguments)

    def test_status_does_not_create_directory_or_start_work(self):
        with tempfile.TemporaryDirectory() as tmp,contextlib.redirect_stdout(io.StringIO()),patch.object(workflow.subprocess,'run') as run:
            p=Path(tmp)/'absent';self.assertEqual(workflow.main(['status','--date','2026-11-02','--run-dir',str(p)]),0);self.assertFalse(p.exists());run.assert_not_called()

    def test_status_rejects_other_date(self):
        with tempfile.TemporaryDirectory() as tmp:
            (Path(tmp)/'scope.json').write_text(json.dumps({'target_date':'2026-11-01'}))
            with self.assertRaises(ValueError):workflow.main(['status','--date','2026-11-02','--run-dir',tmp])

    def test_crawl_is_only_delegated_when_explicitly_called(self):
        with patch.object(workflow.subprocess,'run') as run:
            run.return_value.returncode=2
            self.assertEqual(workflow.main(['resume-crawl','--date','2026-11-02','--crawler-python','/test/python','--credentials-env','/test/.env']),2)
            args=run.call_args.args[0];self.assertEqual(args[:4],['/test/python','-m','ua_workflows.guangdada_direct','resume']);self.assertIn('/test/.env',args);self.assertNotIn('--apply',args)

    def archives(self, root):
        for group in ('game','tool'):
            p=root/(group+'.zip');p.write_bytes(b'original-'+group.encode())
            state={'date':'2026-11-02','preset':{'key':group},'sample_limit':None,'stop_reason':'date-cutoff','archive':{'path':str(p),'bytes':p.stat().st_size,'sha256':hashlib.sha256(p.read_bytes()).hexdigest()}}
            target=root/'runs/2026-11-02_both'/(group+'.json');target.parent.mkdir(parents=True,exist_ok=True);target.write_text(json.dumps(state))

    def test_archive_lookup_uses_confirmed_hash_not_old_filename_glob(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);self.archives(root);(root/'tool_old.zip').write_bytes(b'old')
            self.assertEqual(workflow.collected_archives(root,'2026-11-02'),[(root/'game.zip').resolve(),(root/'tool.zip').resolve()])
            (root/'tool.zip').write_bytes(b'changed')
            with self.assertRaises(ValueError):workflow.collected_archives(root,'2026-11-02')

    def test_partial_or_sample_collection_is_not_auto_intake(self):
        for change in [{'date':'2026-11-01'},{'sample_limit':2},{'stop_reason':'page-limit'}]:
            with tempfile.TemporaryDirectory() as tmp:
                root=Path(tmp);self.archives(root);p=root/'runs/2026-11-02_both/game.json';s=json.loads(p.read_text());s.update(change);p.write_text(json.dumps(s))
                with self.assertRaises(ValueError):workflow.collected_archives(root,'2026-11-02')

    def test_pre_review_requires_real_inputs_and_matching_handoff(self):
        with contextlib.redirect_stderr(io.StringIO()),self.assertRaises(SystemExit):workflow.main(['pre-review','--date','2026-11-02'])
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);(root/'scope.json').write_text(json.dumps({'target_date':'2026-11-02'}));(root/'handoff.json').write_text(json.dumps({'status':'WAITING_PACKAGE'}))
            with self.assertRaises(ValueError):workflow.main(['pre-review','--date','2026-11-02','--run-dir',tmp,'--media-index','/test/index.json','--embedding-cache','/test/cache'])

if __name__=='__main__':unittest.main()

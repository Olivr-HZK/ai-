import unittest
from unittest.mock import Mock
from ua_workflows.replica_studio_api import BASE_URL,ReplicaAPI,ReplicaError


class RecycleTests(unittest.TestCase):
    def client(self,value,status=200):
        transport=Mock();response=Mock(status_code=status,headers={})
        response.json.return_value=value;transport.request.return_value=response
        api=ReplicaAPI(transport);api.account='fixture';api.csrf='fixture'
        return api,transport

    def test_exact_job_revision_recoverable_receipt(self):
        api,t=self.client({'deleted_id':'job-a','next_id':None,'recoverable':True})
        api.recycle_failed_job({'id':'job-a','status':'error','revision':42},confirmed=True)
        self.assertEqual(t.request.call_args.args,('DELETE',BASE_URL+'/api/jobs/job-a'))
        self.assertEqual(t.request.call_args.kwargs['json'],{'revision':42})
        self.assertEqual(t.request.call_count,1)

    def test_unconfirmed_running_and_results_are_not_deleted(self):
        api,t=self.client({})
        for job,confirmed in [({'id':'job-a','status':'error','revision':42},False),
                              ({'id':'job-a','status':'replacing_logo','revision':42},True),
                              ({'id':'job-a','status':'error','revision':42,'logo_replace_view':{'result_file':'x'}},True)]:
            with self.assertRaises(ReplicaError):api.recycle_failed_job(job,confirmed=confirmed)
        t.request.assert_not_called()

    def test_unknown_mutations_are_never_replayed(self):
        for value,status in [({},503),({'deleted_id':'other','recoverable':True},200)]:
            api,t=self.client(value,status)
            with self.assertRaises(ReplicaError) as e:api.recycle_failed_job({'id':'job-a','status':'error','revision':42},confirmed=True)
            self.assertEqual(e.exception.code,'result_unknown');self.assertEqual(t.request.call_count,1)

    def test_delete_rejects_broad_endpoints_and_extra_body_fields(self):
        api,t=self.client({})
        for path,body in [('/api/projects/project-a',{'revision':1}),('/api/jobs/job-a',{'revision':1,'force':True})]:
            with self.assertRaises(ReplicaError):api.request('DELETE',path,body=body)
        t.request.assert_not_called()

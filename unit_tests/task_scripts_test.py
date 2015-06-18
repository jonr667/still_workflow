import unittest, subprocess, os
import ddr_compress.task_server as ts

class TestRawScripts(unittest.TestCase):
    def tearDown(self):
        # clean up files made by script chain
        subprocess.call(['rm', '-rf','/tmp/test*.uv*'], shell=True)
        subprocess.call(['rm', '-rf','test*.uv*'], shell=True)
    def test_script_chain(self):
        basefiles = ['test1.uv','test2.uv','test3.uv']
        for b in basefiles:
            subprocess.call(['do_UV.sh', b, 'localhost:%s/data/%s' % (os.getcwd(),b)])
            self.assertTrue(os.path.exists(b))
            subprocess.call(['do_UVC.sh',b])
            self.assertTrue(os.path.exists(b+'c'))
            subprocess.call(['do_CLEAN_UV.sh',b])
            self.assertFalse(os.path.exists(b))
            subprocess.call(['do_UVCR.sh',b+'c'])
            self.assertTrue(os.path.exists(b+'cR'))
            subprocess.call(['do_CLEAN_UVC.sh',b+'c'])
            self.assertFalse(os.path.exists(b+'c'))
        os.rename('test1.uvcR','/tmp/test1.uvcR')
        os.rename('test3.uvcR','/tmp/test3.uvcR')
        subprocess.call(['do_ACQUIRE_NEIGHBORS.sh', 'localhost:/tmp/test1.uvcR','localhost:/tmp/test3.uvcR'])
        self.assertTrue(os.path.exists('test1.uvcR'))
        self.assertTrue(os.path.exists('test3.uvcR'))
        subprocess.call(['do_UVCRE.sh'] + [b+'cR' for b in basefiles])
        self.assertTrue(os.path.exists('test2.uvcRE'))
        subprocess.call(['do_NPZ.sh', 'test2.uvcRE'])
        self.assertTrue(os.path.exists('test2.uvcRE.npz'))
        subprocess.call(['do_UVCRR.sh', 'test2.uvcR'])
        self.assertTrue(os.path.exists('test2.uvcRR'))
        subprocess.call(['do_NPZ_POT.sh', 'test2.uvcRE.npz', 'localhost:/tmp'])
        self.assertTrue(os.path.exists('/tmp/test2.uvcRE.npz'))
        subprocess.call(['do_CLEAN_UVCRE.sh','test2.uvcRE'])
        self.assertFalse(os.path.exists('test2.uvcRE'))
        subprocess.call(['do_UVCRRE.sh', 'test1.uvcR','test2.uvcRR','test3.uvcR'])
        self.assertTrue(os.path.exists('test2.uvcRRE'))
        subprocess.call(['do_CLEAN_UVCRR.sh','test2.uvcRR'])
        self.assertFalse(os.path.exists('test2.uvcRR'))
        subprocess.call(['do_CLEAN_NPZ.sh','test2.uvcRE.npz'])
        self.assertFalse(os.path.exists('test2.uvcRE.npz'))
        subprocess.call(['do_CLEAN_NEIGHBORS.sh','test1.uvcR','test3.uvcR'])
        self.assertFalse(os.path.exists('test1.uvcR'))
        self.assertFalse(os.path.exists('test3.uvcR'))
        subprocess.call(['do_UVCRRE_POT.sh', 'test2.uvcRRE', 'localhost:/tmp'])
        self.assertTrue(os.path.exists('/tmp/test2.uvcRRE'))
        subprocess.call(['do_CLEAN_UVCR.sh','test2.uvcR'])
        self.assertFalse(os.path.exists('test2.uvcR'))
        subprocess.call(['do_CLEAN_UVCRRE.sh','test2.uvcRRE'])
        self.assertFalse(os.path.exists('test2.uvcRRE'))

if __name__ == '__main__':
    unittest.main()

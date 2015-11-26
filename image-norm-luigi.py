import os
import luigi

import skimage
from skimage import io
from skimage.color import rgb2gray


class ConvertToGrayscale(luigi.Task):
    def run(self):
        for f in self.input():
            try:
                img=io.imread(f.fn)
                newimg=rgb2gray(img)
                io.imsave(f.fn+'.grey',newimg)
            except ValueError:
                print 'Error loading image: %s' % f.fn

    def output(self):
        for f in self.input():
            yield luigi.LocalTarget('%s.grey' % f.fn)
    
    def requires(self):
        return ListImages()

class ListImages(luigi.Task):
    folder=luigi.Parameter()
    
    def run(self):
        f = self.output().open('w')
        for root, subdirs, files in os.walk(self.folder):
            for filename in files:
                if(filename.endswith('jpg') or filename.endswith('gif')):
                    f.write(os.path.join(root, filename)+'\n')
        
    def output(self):
        for root, subdirs, files in os.walk(self.folder):
            for filename in files:
                if(filename.endswith('jpg') or filename.endswith('gif')):
                    yield luigi.LocalTarget(os.path.join(root, filename))

if __name__ == "__main__":
    luigi.run(['ConvertToGrayscale', '--ListImages-folder', 'D:/Data/CorSearch/FullExport_EU_00007_20140605','--local-scheduler'])
    

class FakeStream(object):
    def __init__(self, size=1024):
        self.size = size
        self.current_pos = 0
        self.closed = False

    def read(self, size=0):
        if self.closed:
            raise RuntimeError('The stream is closed')
        if self.current_pos == self.size - 1:
            return ''
        if size == -1 or self.current_pos + size > self.size:
            ret = 'A'*(self.size - self.current_pos)
            self.current_pos = self.size - 1
            return ret
        self.current_pos += size
        return 'A'*size

    def next(self):
        if self.current_pos == self.size:
            raise StopIteration()
        self.current_pos += 1
        return 'A'

    def __iter__(self):
        return self

    def __len__(self):
        return self.size

    def close(self):
        self.closed = True

"""Channel testing"""

# pylint: disable=redefined-outer-name, import-error, missing-function-docstring, missing-class-docstring, invalid-name, attribute-defined-outside-init, unused-import

from multiprocessing import Process
from threading import Thread
import time
import pytest

from maize.core.channels import ChannelFull, ChannelException
from maize.utilities.utilities import has_file


class Test_Channel:
    def test_channel_active(self, loaded_channel):
        assert loaded_channel.active

    def test_channel_ready(self, loaded_channel):
        assert loaded_channel.ready

    def test_channel_ready_multi(self, loaded_channel):
        assert loaded_channel.ready
        assert loaded_channel.ready

    def test_channel_close(self, loaded_channel):
        loaded_channel.close()
        assert not loaded_channel.active
        assert loaded_channel.ready

    def test_empty_channel_ready(self, empty_channel):
        assert empty_channel.active
        assert not empty_channel.ready

    def test_empty_channel_close(self, empty_channel):
        empty_channel.close()
        assert not empty_channel.active
        assert not empty_channel.ready


class Test_DataChannel:
    def test_channel_send(self, empty_datachannel):
        empty_datachannel.send(42)
        assert empty_datachannel.ready

    def test_channel_send_full(self, loaded_datachannel):
        with pytest.raises(ChannelFull):
            loaded_datachannel.send(42, timeout=1)

    def test_channel_receive(self, loaded_datachannel):
        assert loaded_datachannel.receive() == 42

    def test_channel_receive_empty(self, empty_datachannel):
        assert empty_datachannel.receive(timeout=1) is None

    def test_channel_receive_buffer(self, loaded_datachannel):
        assert loaded_datachannel.ready
        assert loaded_datachannel.receive() == 42

    def test_channel_receive_buffer_multi(self, loaded_datachannel):
        assert loaded_datachannel.ready
        assert loaded_datachannel.ready
        assert loaded_datachannel.receive() == 42

    def test_channel_close_receive(self, loaded_datachannel):
        loaded_datachannel.close()
        assert loaded_datachannel.receive() == 42

    def test_channel_receive_multi(self, loaded_datachannel2):
        assert loaded_datachannel2.receive() == 42
        assert loaded_datachannel2.receive() == -42

    def test_channel_receive_multi_buffer(self, loaded_datachannel2):
        assert loaded_datachannel2.ready
        assert loaded_datachannel2.receive() == 42
        assert loaded_datachannel2.receive() == -42

    def test_channel_receive_multi_buffer2(self, loaded_datachannel2):
        assert loaded_datachannel2.ready
        assert loaded_datachannel2.ready
        assert loaded_datachannel2.receive() == 42
        assert loaded_datachannel2.receive() == -42

    def test_channel_receive_multi_close(self, loaded_datachannel2):
        loaded_datachannel2.close()
        assert loaded_datachannel2.ready
        assert loaded_datachannel2.receive() == 42
        assert loaded_datachannel2.receive() == -42


class Test_FileChannel:
    def test_channel_size_empty(self, empty_filechannel):
        assert empty_filechannel.size == 0

    def test_channel_size_full(self, loaded_filechannel):
        assert loaded_filechannel.size == 1

    def test_channel_send(self, empty_filechannel, datafile):
        empty_filechannel.send(datafile)
        assert empty_filechannel.ready

    def test_channel_send_no_file(self, empty_filechannel, shared_datadir):
        with pytest.raises(ChannelException):
            empty_filechannel.send(shared_datadir / "nonexistent.abc")

    def test_channel_send_copy(self, empty_filechannel, datafile):
        empty_filechannel.copy = True
        empty_filechannel.send(datafile)
        assert empty_filechannel.ready

    def test_channel_send_full(self, loaded_filechannel, datafile):
        with pytest.raises(ChannelFull):
            loaded_filechannel.send(datafile, timeout=1)

    def test_channel_send_full_payload(self, empty_filechannel, datafile):
        """
        Test situation where data was put in the payload queue, but the
        trigger wasn't set. This shouldn't really happen in practice.
        """
        empty_filechannel._payload.put(datafile)
        with pytest.raises(ChannelFull):
            empty_filechannel.send(datafile, timeout=1)

    def test_channel_send_dict(self, empty_filechannel, nested_datafiles_dict):
        empty_filechannel.send(nested_datafiles_dict)
        assert empty_filechannel.ready

    def test_channel_preload_full(self, loaded_filechannel, datafile):
        with pytest.raises(ChannelFull):
            loaded_filechannel.preload(datafile)

    def test_channel_receive(self, loaded_filechannel, datafile):
        path = loaded_filechannel.receive()
        assert path.exists()
        assert path.name == datafile.name

    def test_channel_receive_trigger(self, empty_filechannel):
        empty_filechannel._file_trigger.set()
        with pytest.raises(ChannelException):
            empty_filechannel.receive(timeout=1)

    def test_channel_receive_nested_multi(self, loaded_filechannel2, nested_datafiles):
        paths = loaded_filechannel2.receive()
        assert paths[1].parent.name == nested_datafiles[1].parent.name
        for path, datafile in zip(paths, nested_datafiles):
            assert path.exists()
            assert path.name == datafile.name

    def test_channel_receive_nested_multi_link(self, loaded_filechannel3, nested_datafiles):
        paths = loaded_filechannel3.receive()
        assert paths[1].parent.name == nested_datafiles[1].parent.name
        for path, datafile in zip(paths, nested_datafiles):
            assert path.exists()
            assert path.name == datafile.name

    def test_channel_receive_multi(self, loaded_filechannel, datafile2):
        path = loaded_filechannel.receive()
        loaded_filechannel.send(datafile2)
        path = loaded_filechannel.receive()
        assert path.exists()
        assert path.name == datafile2.name

    def test_channel_receive_dict(self, loaded_filechannel4, nested_datafiles_dict):
        paths = loaded_filechannel4.receive()
        assert paths.keys() == nested_datafiles_dict.keys()
        for key in nested_datafiles_dict:
            assert paths[key].exists()
            assert paths[key].name == nested_datafiles_dict[key].name

    def test_channel_receive_empty(self, empty_filechannel):
        assert empty_filechannel.receive(timeout=1) is None

    def test_channel_receive_buffer(self, loaded_filechannel):
        assert loaded_filechannel.ready
        assert loaded_filechannel.receive().exists()

    def test_channel_receive_buffer_multi(self, loaded_filechannel):
        assert loaded_filechannel.ready
        assert loaded_filechannel.ready
        assert loaded_filechannel.receive().exists()

    def test_channel_receive_auto_preload(self, loaded_filechannel5, datafile):
        path, *_ = loaded_filechannel5.receive()
        assert path.exists()
        assert path.name == datafile.name

    def test_channel_close_receive(self, loaded_filechannel):
        loaded_filechannel.close()
        assert loaded_filechannel.receive().exists()

    def test_channel_close_timeout(self, empty_filechannel, datafile):
        def _clear():
            time.sleep(5)
            empty_filechannel._file_trigger.clear()

        empty_filechannel.send(datafile)
        assert empty_filechannel._file_trigger.is_set()
        assert has_file(empty_filechannel._channel_dir)
        proc = Process(target=_clear)
        proc.start()
        empty_filechannel.close()
        proc.join()
        assert not empty_filechannel._file_trigger.is_set()

    def test_channel_flush(self, loaded_filechannel, datafile):
        path = loaded_filechannel.flush()
        assert path[0].exists()
        assert path[0].name == datafile.name

    def test_channel_flush_empty(self, empty_filechannel):
        path = empty_filechannel.flush()
        assert path == []

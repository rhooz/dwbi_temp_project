import os

try:
    if "REMOTE_DEBUG" in os.environ and "REMOTE_DEBUG_PORT" in os.environ:
        import pydevd

        print("starting remote debug trace")
        print("pydevd.settrace("+str(os.environ['REMOTE_DEBUG'])+", port="+str(os.environ['REMOTE_DEBUG_PORT']).strip()+", stdoutToServer=True, stderrToServer=True, suspend=False)")
        pydevd.settrace(str(os.environ['REMOTE_DEBUG']), port=int(os.environ['REMOTE_DEBUG_PORT']), stdoutToServer=True, stderrToServer=True,
                        suspend=False)
except Exception as e:
    print("Remote debugging not configured properly: " + str(e))
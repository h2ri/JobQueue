class Job():
    def __init__(self, name, memory=None, env=None, cmd_param=None):
        self.container_name = name
        self.cmd_param = cmd_param or []
        self.env = memory or {}
        self.memory_requirment = memory or {}

# Embulk Operator on based on standard BashOperator
# xcom_pulls the query embulk from an upstream task. 
# Consult https://www.embulk.org/docs/ for embulk query
import os
import signal
from subprocess import Popen, STDOUT, PIPE
from tempfile import gettempdir, NamedTemporaryFile

from builtins import bytes

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.utils.file import TemporaryDirectory
from airflow.utils.operator_helpers import context_to_airflow_vars


class EmbulkOperator(BaseOperator):
    template_fields = ('bash_command', 'env')
    template_ext = ('.sh', '.bash',)
    ui_color = '#f0ede4'

    @apply_defaults
    def __init__(
            self,
            bash_command=' ',
            xcom_push=False,
            env=None,
            output_encoding='utf-8',
            input_task_id='',
            *args, **kwargs):

        super(EmbulkOperator, self).__init__(*args, **kwargs)
        self.bash_command = bash_command
        self.env = env
        self.xcom_push_flag = xcom_push
        self.output_encoding = output_encoding
        self.input_task_id = input_task_id

    def execute(self, context):

        embulk_command = context['ti'].xcom_pull(task_ids=self.input_task_id, key='query_embulk')
        #self.bash_command = embulk_command

        self.log.info("Tmp dir root location: \n %s", gettempdir())

        # Prepare env for child process.
        if self.env is None:
            self.env = os.environ.copy()
        airflow_context_vars = context_to_airflow_vars(context, in_env_var_format=True)
        self.log.info("Exporting the following env vars:\n" +
                      '\n'.join(["{}={}".format(k, v)
                                 for k, v in
                                 airflow_context_vars.items()]))
        self.env.update(airflow_context_vars)

        self.lineage_data = embulk_command

        with TemporaryDirectory(prefix='airflowtmp') as tmp_dir:
            with NamedTemporaryFile(dir=tmp_dir, prefix=self.task_id) as f:

                f.write(bytes(embulk_command, 'utf_8'))
                f.flush()
                fname = f.name
                script_location = os.path.abspath(fname)
                self.log.info(
                    "Temporary script location: %s",
                    script_location
                )

                def pre_exec():
                    # Restore default signal disposition and invoke setsid
                    for sig in ('SIGPIPE', 'SIGXFZ', 'SIGXFSZ'):
                        if hasattr(signal, sig):
                            signal.signal(getattr(signal, sig), signal.SIG_DFL)
                    os.setsid()

                self.log.info("Running command: %s", embulk_command)
                sp = Popen(
                    ['bash', fname],
                    stdout=PIPE, stderr=STDOUT,
                    cwd=tmp_dir, env=self.env,
                    preexec_fn=pre_exec)

                self.sp = sp

                self.log.info("Output:")
                line = ''
                for line in iter(sp.stdout.readline, b''):
                    line = line.decode(self.output_encoding).rstrip()
                    self.log.info(line)
                sp.wait()
                self.log.info(
                    "Command exited with return code %s",
                    sp.returncode
                )

                if sp.returncode:
                    raise AirflowException("Bash command failed")

        if self.xcom_push_flag:
            return line


    def on_kill(self):
        self.log.info('Sending SIGTERM signal to bash process group')
        os.killpg(os.getpgid(self.sp.pid), signal.SIGTERM)



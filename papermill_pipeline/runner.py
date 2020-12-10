from os.path import join, basename
from typing import NamedTuple, Dict, Any, Optional, List

import papermill as pm


TYPE_DICT = {
    'int': int,
    'float': float,
    'str': str,
    'bool': bool,
}


class ParseRunnerException(Exception):
    pass


class NotebookTask(NamedTuple):
    notebook: str
    inputs: Optional[Dict[str, Any]]
    outputs: Optional[Dict[str, Any]] = None
    parameters: Optional[Dict[str, Any]] = None
    name: Optional[str] = None

    def run(self,
            inputs: Optional[Dict[str, Any]] = None,
            outputs: Optional[Dict[str, Any]] = None,
            parameters: Optional[Dict[str, Any]] = None,
            work_dir: str = '.',
            progress_bar: bool = False,
            kernel_name=None):
        inputs = {**(self.inputs or {}), **(inputs or {})}
        outputs = {**(self.outputs or {}), **(outputs or {})}
        parameters = {**(self.parameters or {}), **(parameters or {})}

        parameters = {
            **inputs, **outputs, **parameters
        }

        out_name = join(work_dir, basename(self.notebook))

        pm.execute_notebook(
            self.notebook,
            out_name,
            cwd=work_dir,
            progress_bar=progress_bar,
            parameters=parameters,
            kernel_name=kernel_name
        )


def print_task(number: int, task: NotebookTask):
    def _print_defin(defin_name: str, defin: Dict):
        if defin:
            print(f'  {defin_name}:')
            for name, value in defin.items():
                print(f'    {name}: {value}')

    print(f'Task #{number}: {task.name}')
    print(f'  notebook: {task.notebook}')
    _print_defin('inputs', task.inputs)
    _print_defin('outputs', task.outputs)
    _print_defin('parameters', task.parameters)
    print()


def parse_params_strs(params_strs: List[str]) -> Dict[str, str]:
    params = dict()
    for p in params_strs:
        tokens = p.split('=')
        params[tokens[0]] = '='.join(tokens[1:])
    return params


def parse_def(name: str, p_def: Dict, params: Dict):
    if p_def.get('required', False) and name not in params:
        raise ParseRunnerException(f'{name} is required')
    value = params.get(name)

    type_n = p_def.get('type', 'str')
    type_t = TYPE_DICT[type_n]

    if value is None:
        value = p_def.get('default')

    if value is not None:
        value = type_t(value)
    return value


def parse_params(params_defs: Dict, params_strs: List[str])->Dict:
    params = parse_params_strs(params_strs)

    for name, p_def in params_defs.items():
        params[name] = parse_def(name, p_def, params)
    return params


####


def parse_task(task_name: str, task_def: Dict, tasks: Dict[str, NotebookTask], params: Dict, notebooks_dir: str):
    def link(defin):
        _res = {}
        for key, value in defin.items():
            if not isinstance(value, dict):
                # Link of value
                if value.startswith('$params.'):
                    lookup = value.replace('$params.', '')
                    if lookup not in params:
                        raise ParseRunnerException(f'Task {task_name}: Cannot find "{lookup}" in parameters')
                    _res[key] = params[lookup]
                elif value.startswith('$'):
                    name, handle_name, *ts = value.split('.')
                    name = name[1:]
                    p_name = '.'.join(ts)
                    linked_task = tasks[name]
                    handle = getattr(linked_task, handle_name)
                    if p_name not in handle:
                        raise ParseRunnerException(
                            f'Task {task_name}: Cannot find "{value}" in task {name}.{handle_name}')
                    _res[key] = handle[p_name]
                else:
                    _res[key] = value
            else:
                # param definition
                full_param_name = f'{task_name}.{key}'
                _res[key] = parse_def(full_param_name, value, params)
        return _res

    notebook = join(notebooks_dir, task_def['notebook'])
    inputs = link(task_def.get('inputs') or {})
    outputs = link(task_def.get('outputs') or {})
    parameters = link(task_def.get('parameters') or {})

    return NotebookTask(
        name=task_name,
        notebook=notebook,
        inputs=inputs,
        outputs=outputs,
        parameters=parameters,
    )


def parse_tasks(tasks_defs: Dict, params: Dict, notebooks_dir: str) -> List[NotebookTask]:
    tasks = {}
    res = []

    for name, task_def in tasks_defs.items():
        task = parse_task(name, task_def, tasks, params, notebooks_dir)
        res.append(task)
        tasks[name] = task
    return res




















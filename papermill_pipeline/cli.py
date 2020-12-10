import argparse
import sys
from os import makedirs
from os.path import dirname, join, basename
from typing import Tuple, List

import yaml

from papermill_pipeline.runner import parse_params, ParseRunnerException, parse_tasks, print_task, NotebookTask


def main():
    p = argparse.ArgumentParser()

    p.add_argument('pipeline', default='pipeline.yaml')
    p.add_argument('--work-dir', default='.')
    p.add_argument('--end-task', '-e', required=False, default=-1, type=int)
    p.add_argument('--begin-task', '-b', required=False, default=-1, type=int)
    p.add_argument('--exclude', '-x', nargs='+', type=int, help="Exclude tasks")
    p.add_argument('--dry-run', '-n', required=False, action='store_true')
    p.add_argument('--verbose', '-v', default=0, type=int, required=False)
    p.add_argument('--progress', action='store_true')
    p.add_argument('--kernel-name', default='python3')
    p.add_argument('-p', '--param', action='append')

    args = p.parse_args()
    with open(args.pipeline) as f:
        pipeline_yaml = yaml.safe_load(f)

    try:
        params = parse_params(pipeline_yaml['params'], args.param or [])
    except ParseRunnerException as ex:
        sys.stderr.write(f'Error while parsing params: {ex}')
        sys.exit(1)

    try:
        tasks: List[Tuple[int, NotebookTask]]
        tasks = list(enumerate(parse_tasks(pipeline_yaml['tasks'], params, notebooks_dir=dirname(args.pipeline))))
    except ParseRunnerException as ex:
        sys.stderr.write(f'Error while parsing tasks: {ex}')
        sys.exit(1)

    if args.end_task > -1:
        tasks = tasks[:args.end_task]

    if args.begin_task > -1:
        tasks = tasks[args.begin_task - 1:]

    tasks = [
        (i, t)
        for i, t in tasks
        if i + 1 not in set(args.exclude or [])
    ]

    if args.dry_run:
        print(f'Work dir: {args.work_dir}')
        for i, t in tasks:
            print_task(i + 1, t)
        return

    if args.verbose >= 1:
        print(f'Work dir: {args.work_dir}')

    makedirs(join(args.work_dir, 'data'), exist_ok=True)
    for i, t in tasks:
        if args.verbose >= 2:
            print_task(i + 1, t)
        elif args.verbose >= 1:
            print(f'#{i + 1} {t.name}: {basename(t.notebook)} ({args.kernel_name})')

        t.run(
            work_dir=args.work_dir,
            progress_bar=args.progress,
            kernel_name=args.kernel_name,
        )


if __name__ == '__main__':
    main()

import example
import dagger.dag

def main():
  dag,rules = example.build_dag()
  dagger.dag.rundag(dag,rules)

if __name__=='__main__':
  main()
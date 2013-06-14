package proj.zoie.api.indexing;


/**
 * @author Dmytro Ivchenko
 */
public class NoOpOptimizeScheduler extends OptimizeScheduler
{
  @Override
  public OptimizeType getScheduledOptimizeType()
  {
    return OptimizeType.NONE;
  }

  @Override
  public void finished()
  {
  }

  @Override
  public void shutdown()
  {
  }
}

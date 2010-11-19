class Player 
  attr_reader :id, :x, :y;
  
  def initialize(id)
    @id,@x,@y = id,(rand * 20),(rand * 20)
  end
  
  def move
    dx,dy = ((rand * 2)-1),((rand * 2)-1)
    @x += dx
    @y += dy
    @x = (@x > 20) ? 20 : ((@x < 0) ? 0 : @x)
    @y = (@y > 20) ? 20 : ((@y < 0) ? 0 : @y)
  end
  
  def to_s
    "#{@x},#{@y},#{@id}"
  end
end

players = Array.new;

step_count = 1000000
player_count = 1000
departure_rate = 0.0005
player_id = 0;

(0...step_count).each do |step|
  if((Math.exp(-1 * players.length / player_count)) > rand) then
    player = Player.new(player_id);
    players.push(player);
    player_id += 1;
    print "+,#{player}\n"
  elsif ((1-departure_rate)**(players.length) < rand) then
    player = players.delete_at(rand(players.length))
    print "-,#{player}\n"
  else
    player = players[rand(players.length)]
    print "-,#{player}\n"
    player.move;
    print "+,#{player}\n"
  end
end